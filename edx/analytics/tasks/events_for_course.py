
from collections import defaultdict
import json
import math
import os

import luigi

import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import url_path_join, get_target_from_url


class EventsForCourses(EventLogSelectionMixin, MultiOutputMapReduceJobTask):

    course_ids = luigi.Parameter(is_list=True, default=[])
    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        if 'edx.course.enrollment' not in event['event_type']:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        course_id = event_data['course_id']

        if course_id in self.course_ids:
            yield course_id, line

    def multi_output_reducer(self, _key, values, output_file):
        for value in values:
            output_file.write(value.strip())
            output_file.write('\n')

    def output_path_for_key(self, key):
        return url_path_join(self.output_root, key.replace('/','_') + '.log')


class EventsForUsers(EventLogSelectionMixin, MapReduceJobTask):

    usernames = luigi.Parameter(is_list=True, default=[])
    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event['username']
        if username in self.usernames:
            yield username, line.strip()

    def output(self):
        return get_target_from_url(self.output_root + '/')


class FirstEnrollments(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        if 'edx.course.enrollment.activated' not in line:
            return

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        if event['event_type'] != 'edx.course.enrollment.activated':
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        yield (event_data['course_id'], event_data['user_id']), event['time']

    def reducer(self, key, timestamps):
        course_id, user_id = key
        yield course_id, user_id, sorted(timestamps)[0]

    def output(self):
        return get_target_from_url(self.output_root + '/')


NULL_VALUE = u'\\N'
class EventStatistics(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        metadata = {}
        metadata['filename'] = os.environ['map_input_file']
        metadata['size'] = len(line.strip())

        metadata['offset'] = os.environ.get('map_input_start', NULL_VALUE)
        metadata['length'] = os.environ.get('map_input_length', NULL_VALUE)
        metadata['event_type'] = event.get('event_type', NULL_VALUE)
        metadata['username'] = event.get('username', NULL_VALUE)
        metadata['course_id'] = event.get('context', {}).get('course_id', NULL_VALUE)
        metadata['time'] = event['time'].split('T')[1].split('+')[0].split('.')[0]

        yield (date_string, metadata['username'].encode('utf8')), json.dumps(metadata)

    def reducer(self, map_key, values):
        date_string = map_key[0]
        username = map_key[1].decode('utf8')

        stats = {}
        for stat_key in ['course_id', 'filename', 'hour', 'location_in_file', 'total']:
            stats[stat_key] = defaultdict(EventStats)

        for metadata_string in values:
            metadata = json.loads(metadata_string)

            course_id = metadata['course_id']
            hour_of_day = int(metadata['time'].split(':')[0])
            if metadata['offset'] == NULL_VALUE or metadata['length'] == NULL_VALUE:
                location_in_file = NULL_VALUE
            else:
                location_in_file = math.floor((float(metadata['offset']) * 10) / (float(metadata['length'])))

            stats['course_id'][course_id] += metadata
            stats['filename'][metadata['filename']] += metadata
            stats['hour'][hour_of_day] += metadata
            stats['location_in_file'][location_in_file] += metadata
            stats['total'][NULL_VALUE] += metadata

        for stat_key, values in stats.iteritems():
            for key, event_stats in values.iteritems():
                yield date_string, username, stat_key, key, event_stats.count, event_stats.size

    def output(self):
        return get_target_from_url(self.output_root + '/')


class EventStats(object):

    def __init__(self):
        self.count = 0
        self.size = 0

    def __iadd__(self, metadata):
        self.count += 1
        self.size += metadata['size']
        return self


class GrepLogs(EventLogSelectionMixin, MultiOutputMapReduceJobTask):

    patterns = luigi.Parameter(is_list=True, default=[])
    output_root = luigi.Parameter()

    def mapper(self, line):
        for idx, pattern in enumerate(self.patterns):
            if pattern in line:
                return idx, line

    def multi_output_reducer(self, _key, values, output_file):
        for value in values:
            output_file.write(value.strip())
            output_file.write('\n')

    def output_path_for_key(self, key):
        return url_path_join(self.output_root, 'pattern_{}'.format(key))
