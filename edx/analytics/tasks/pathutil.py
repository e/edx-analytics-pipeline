"""
Helper classes to specify file dependencies for input and output.

Supports inputs from S3 and local FS.
Supports outputs to HDFS, S3, and local FS.

"""

import boto
import datetime
import fnmatch
import logging
import os
import re

import luigi
import luigi.hdfs
import luigi.format
import luigi.task

from luigi.date_interval import DateInterval

from edx.analytics.tasks.s3_util import generate_s3_sources, get_s3_bucket_key_names
from edx.analytics.tasks.url import ExternalURL, UncheckedExternalURL, url_path_join, get_target_from_url
from edx.analytics.tasks.util import eventlog


log = logging.getLogger(__name__)


class PathSetTask(luigi.Task):
    """
    A task to select a subset of files in an S3 bucket or local FS.

    Parameters:

      src: a URL pointing to a folder in s3:// or local FS.
      include:  a list of patterns to use to select.  Multiple patterns are OR'd.
      manifest: a URL pointing to a manifest file location.
    """
    src = luigi.Parameter(
        default_from_config={'section': 'event-logs', 'name': 'source'}
    )
    include = luigi.Parameter(is_list=True, default=('*',))
    manifest = luigi.Parameter(default=None)

    def __init__(self, *args, **kwargs):
        super(PathSetTask, self).__init__(*args, **kwargs)
        self.s3_conn = None

    def generate_file_list(self):
        """Yield each individual path given a source folder and a set of file-matching expressions."""
        if self.src.startswith('s3'):
            # connect lazily as needed:
            if self.s3_conn is None:
                self.s3_conn = boto.connect_s3()
            for _bucket, _root, path in generate_s3_sources(self.s3_conn, self.src, self.include):
                source = url_path_join(self.src, path)
                yield ExternalURL(source)
        else:
            # Apply the include patterns to the relative path below the src directory.
            for dirpath, _dirnames, files in os.walk(self.src):
                for filename in files:
                    filepath = os.path.join(dirpath, filename)
                    relpath = os.path.relpath(filepath, self.src)
                    if any(fnmatch.fnmatch(relpath, include_val) for include_val in self.include):
                        yield ExternalURL(filepath)

    def manifest_file_list(self):
        """Write each individual path to a manifest file and yield the path to that file."""
        manifest_target = get_target_from_url(self.manifest)
        if not manifest_target.exists():
            with manifest_target.open('w') as manifest_file:
                for external_url_task in self.generate_file_list():
                    manifest_file.write(external_url_task.url + '\n')

        yield ExternalURL(self.manifest)

    def requires(self):
        if self.manifest is not None:
            return self.manifest_file_list()
        else:
            return self.generate_file_list()

    def complete(self):
        # An optimization: just declare that the task is always
        # complete, by definition, because it is whatever files were
        # requested that match the filter, not a set of files whose
        # existence needs to be checked or generated again.
        return True

    def output(self):
        return [task.output() for task in self.requires()]


class EventLogSelectionDownstreamMixin(object):
    """Defines parameters for passing upstream to tasks that use EventLogSelectionMixin."""

    source = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'event-logs', 'name': 'source'}
    )
    interval = luigi.DateIntervalParameter()
    expand_interval = luigi.TimeDeltaParameter(
        default_from_config={'section': 'event-logs', 'name': 'expand_interval'}
    )
    pattern = luigi.Parameter(
        default_from_config={'section': 'event-logs', 'name': 'pattern'}
    )


class EventLogSelectionTask(EventLogSelectionDownstreamMixin, luigi.WrapperTask):
    """
    Select all relevant event log input files from a directory.

    Recursively list all files in the directory which is expected to contain the input files organized in such a way
    that a pattern can be used to find them. Filenames are expected to contain a date which represents an approximation
    of the date found in the events themselves.

    Parameters:
        source: A URL to a path that contains log files that contain the events.
        interval: The range of dates to export logs for.
        expand_interval: A time interval to add to the beginning and end of the interval to expand the windows of files
            captured.
        pattern: A regex with a named capture group for the date that approximates the date that the events within were
            emitted. Note that the search interval is expanded, so events don't have to be in exactly the right file
            in order for them to be processed.
    """

    def __init__(self, *args, **kwargs):
        super(EventLogSelectionTask, self).__init__(*args, **kwargs)
        self.interval = DateInterval(
            self.interval.date_a - self.expand_interval,
            self.interval.date_b + self.expand_interval
        )
        self.requirements = None

    def requires(self):
        # This method gets called several times. Avoid making multiple round trips to S3 by caching the first result.
        if self.requirements is None:
            log.debug('No saved requirements found, refreshing requirements list.')
            self.requirements = self._get_requirements()
        else:
            log.debug('Using cached requirements.')
        return self.requirements

    def _get_requirements(self):
        """
        Gather the set of requirements needed to run the task.

        This can be a rather expensive operation that requires usage of the S3 API to list all files in the source
        bucket and select the ones that are applicable to the given date range.
        """
        url_gens = []
        for source in self.source:
            if source.startswith('s3'):
                url_gens.append(self._get_s3_urls(source))
            else:
                url_gens.append(self._get_local_urls(source))

        log.debug('Matching urls using pattern="%s"', self.pattern)
        log.debug(
            'Date interval: %s <= date < %s', self.interval.date_a.isoformat(), self.interval.date_b.isoformat()
        )

        return [UncheckedExternalURL(url) for url_gen in url_gens for url in url_gen if self.should_include_url(url)]

    def _get_s3_urls(self, source):
        """Recursively list all files inside the source URL directory."""
        s3_conn = boto.connect_s3()
        bucket_name, root = get_s3_bucket_key_names(source)
        bucket = s3_conn.get_bucket(bucket_name)
        for key_metadata in bucket.list(root):
            if key_metadata.size > 0:
                key_path = key_metadata.key[len(root):].lstrip('/')
                yield url_path_join(source, key_path)

    def _get_local_urls(self, source):
        """Recursively list all files inside the source directory on the local filesystem."""
        for directory_path, _subdir_paths, filenames in os.walk(source):
            for filename in filenames:
                yield os.path.join(directory_path, filename)

    def should_include_url(self, url):
        """
        Determine whether the file pointed to by the URL should be included in the set of files used for analysis.

        Presently filters first on pattern match and then on the datestamp extracted from the file name.
        """
        match = re.match(self.pattern, url)
        if not match:
            log.debug('Excluding due to pattern mismatch: %s', url)
            return False

        # TODO: support patterns that don't contain a "date" group

        parsed_datetime = datetime.datetime.strptime(match.group('date'), '%Y%m%d')
        parsed_date = datetime.date(parsed_datetime.year, parsed_datetime.month, parsed_datetime.day)
        should_include = parsed_date in self.interval

        if should_include:
            log.debug('Including: %s', url)
        else:
            log.debug('Excluding due to date interval: %s', url)
        return should_include

    def output(self):
        return [task.output() for task in self.requires()]


class EventLogSelectionMixin(EventLogSelectionDownstreamMixin):
    """
    Extract events corresponding to a specified time interval and outputs them from a mapper.

    Parameters:
        source: A URL to a path that contains log files that contain the events.
        interval: The range of dates to export logs for.
        pattern: A regex with a named capture group for the date that approximates the date that the events within were
            emitted. Note that the search interval is expanded, so events don't have to be in exactly the right file
            in order for them to be processed.
    """

    def requires(self):
        """Use EventLogSelectionTask to define inputs."""
        return EventLogSelectionTask(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
        )

    def init_local(self):
        """Convert intervals to date strings for alpha-numeric comparison."""
        super(EventLogSelectionMixin, self).init_local()
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')

    def get_event_and_date_string(self, line):
        """Default mapper implementation, that always outputs the log line, but with a configurable key."""
        event = eventlog.parse_json_event(line)
        if event is None:
            return None

        try:
            event_time = event['time']
        except KeyError:
            self.incr_counter('Event', 'Missing Time Field', 1)
            return None

        # Don't use strptime to parse the date, it is extremely slow
        # to do so. Instead rely on alphanumeric comparisons.  The
        # timestamp is ISO8601 formatted, so dates will look like
        # %Y-%m-%d.  For example: 2014-05-20.
        date_string = event_time.split("T")[0]

        if date_string < self.lower_bound_date_string or date_string >= self.upper_bound_date_string:
            return None

        return event, date_string
