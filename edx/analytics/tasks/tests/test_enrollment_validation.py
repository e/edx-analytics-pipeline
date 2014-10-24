"""Test enrollment computations"""

import json

import luigi

from edx.analytics.tasks.enrollment_validation import (
    CourseEnrollmentValidationTask,
    DEACTIVATED,
    ACTIVATED,
    VALIDATED,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class CourseEnrollmentValidationTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.initialize_ids()

        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentValidationTask(
            interval=fake_param.parse('2013-12-17'),
            output_root='/fake/output'
        )
        self.task.init_local()

        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "username": "test_user",
            "host": "test_host",
            "event_source": "server",
            "event_type": "edx.course.enrollment.activated",
            "context": {
                "course_id": self.course_id,
                "org_id": self.org_id,
                "user_id": self.user_id,
            },
            "time": "{0}+00:00".format(self.timestamp),
            "ip": "127.0.0.1",
            "event": {
                "course_id": self.course_id,
                "user_id": self.user_id,
                "mode": "honor",
            },
            "agent": "blah, blah, blah",
            "page": None
        }
        event_dict.update(**kwargs)
        return event_dict

    def assert_no_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(tuple(self.task.mapper(line)), tuple())

    def test_non_enrollment_event(self):
        line = 'this is garbage'
        self.assert_no_output_for(line)

    def test_unparseable_enrollment_event(self):
        line = 'this is garbage but contains edx.course.enrollment'
        self.assert_no_output_for(line)

    def test_missing_event_type(self):
        event_dict = self._create_event_dict()
        event_dict['old_event_type'] = event_dict['event_type']
        del event_dict['event_type']
        line = json.dumps(event_dict)
        self.assert_no_output_for(line)

    def test_nonenroll_event_type(self):
        line = self._create_event_log_line(event_type='edx.course.enrollment.unknown')
        self.assert_no_output_for(line)

    def test_bad_datetime(self):
        line = self._create_event_log_line(time='this is a bogus time')
        self.assert_no_output_for(line)

    def test_bad_event_data(self):
        line = self._create_event_log_line(event=["not an event"])
        self.assert_no_output_for(line)

    def test_illegal_course_id(self):
        line = self._create_event_log_line(event={"course_id": ";;;;bad/id/val", "user_id": self.user_id})
        self.assert_no_output_for(line)

    def test_missing_user_id(self):
        line = self._create_event_log_line(event={"course_id": self.course_id})
        self.assert_no_output_for(line)

    def test_good_enroll_event(self):
        line = self._create_event_log_line()
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, ACTIVATED, None, None)),)
        self.assertEquals(event, expected)

    def test_good_unenroll_event(self):
        line = self._create_event_log_line(event_type=DEACTIVATED)
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, DEACTIVATED, None, None)),)
        self.assertEquals(event, expected)

    def test_good_validation_event(self):
        line = self._create_event_log_line(event_type=VALIDATED)
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, VALIDATED, None, None)),)
        self.assertEquals(event, expected)


class CourseEnrollmentValidationTaskLegacyMapTest(InitializeLegacyKeysMixin, CourseEnrollmentValidationTaskMapTest):
    """TODO:"""
    pass


class CourseEnrollmentValidationTaskReducerTest(unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.create_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.key = (self.course_id, self.user_id)

    def create_task(self, interval='2013-01-01-2014-10-10'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentValidationTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
            generate_before=True,
        )
        self.task.init_local()

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        expected_with_key = tuple([self.key + value for value in expected])
        self.assertEquals(self._get_reducer_output(inputs), expected_with_key)

    def test_no_events(self):
        self._check_output([], tuple())

    def test_missing_single_enrollment(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
                     '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_enroll_unenroll(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            # missing deactivation (between 4/1 and 9/1)
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(inactive)",
             '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),
            ('2013-04-01T00:00:01.123457', DEACTIVATED, "start => validate(inactive)",
             '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),
        )
        self._check_output(inputs, expected)

    def test_single_enrollment(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_single_unenrollment(self):
        inputs = [
            ('2013-09-01T00:00:01.123457', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-05-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation
        ]
        expected = (('2013-04-01T00:00:01.123456', ACTIVATED, "start => deactivate",
                     '2013-04-01T00:00:01.123456', '2013-05-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_single_unvalidated_unenrollment(self):
        inputs = [
            ('2013-05-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation
        ]
        expected = (('2013-05-01T00:00:01.123455', ACTIVATED, "start => deactivate",
                     None, '2013-05-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_single_enroll_unenroll(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-05-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_single_unenroll_enroll(self):
        inputs = [
            ('2013-09-01T00:00:01.123457', ACTIVATED, None, None),
            ('2013-05-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_multiple_validation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-08-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-07-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-01-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_multiple_validation_without_enroll(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-08-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-07-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            # missing activation
        ]
        expected = (('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
                     '2013-04-01T00:00:01.123456', '2013-07-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_enroll_unenroll_with_validations(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-08-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-07-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_missing_activate_between_validation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            # missing activation
            ('2013-08-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-05-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-08-01T00:00:01.123457', ACTIVATED, "validate(inactive) => validate(active)",
                     '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_deactivate_between_validation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            ('2013-08-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-01-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-08-01T00:00:01.123457', DEACTIVATED, "validate(active) => validate(inactive)",
                     '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_deactivate_from_validation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', ACTIVATED, None, None),
            # missing deactivation
            ('2013-08-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-08-01T00:00:01.123457', DEACTIVATED, "validate(active) => activate",
                     '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_deactivate_from_activation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-04-01T00:00:01.123457', DEACTIVATED, "activate => validate(inactive)",
                     '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_activate_from_deactivation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            # missing activation
            ('2013-08-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-01-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-08-01T00:00:01.123457', ACTIVATED, "deactivate => validate(active)",
                     '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_activate_between_deactivation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation
            ('2013-08-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-01-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-08-01T00:00:01.123457', ACTIVATED, "deactivate => deactivate",
                     '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_deactivate_between_activation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', ACTIVATED, None, None),
            # missing deactivation
            ('2013-01-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-01-01T00:00:01.123457', DEACTIVATED, "activate => activate",
                     '2013-01-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_activate_from_validation(self):
        inputs = [
            ('2013-10-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-08-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-09-01T00:00:01.123457', ACTIVATED, "validate(inactive) => deactivate",
                     '2013-09-01T00:00:01.123456', '2013-10-01T00:00:01.123456'),)
        self._check_output(inputs, expected)


class CourseEnrollmentValidationTaskEventReducerTest(unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.create_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.key = (self.course_id, self.user_id)

    def create_task(self, interval='2013-01-01-2014-10-10'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentValidationTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
            event_output=True,
            generate_before=True,
        )
        self.task.init_local()

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def test_missing_single_enrollment(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        events = self._get_reducer_output(inputs)
        self.assertEquals(len(events), 1)
        event = json.loads(events[0])

        self.assertEquals(event.get('event_type'), ACTIVATED)
        self.assertEquals(event.get('time'), '2013-04-01T00:00:01.123456')

        synthesized = event.get('synthesized')
        self.assertEquals(synthesized.get('reason'), "start => validate(active)")
        self.assertEquals(synthesized.get('after_time'), '2013-04-01T00:00:01.123456')
        self.assertEquals(synthesized.get('before_time'), '2013-09-01T00:00:01.123456')


class GenerateBeforeDisabledTaskReducerTest(unittest.TestCase):
    """
    Tests to verify that events before first validation event are properly skipped.
    """
    def setUp(self):
        self.create_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.key = (self.course_id, self.user_id)

    def create_task(self, interval='2013-01-01-2014-10-10'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentValidationTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
            generate_before=False,
        )
        self.task.init_local()

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        expected_with_key = tuple([self.key + value for value in expected])
        self.assertEquals(self._get_reducer_output(inputs), expected_with_key)

    def test_no_events(self):
        self._check_output([], tuple())

    def test_missing_single_enrollment(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
                     '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_early_single_enrollment(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, True, '2012-04-01T00:00:01.123456'),
            # missing activation (4/1/12)
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_single_deactivation(self):
        inputs = [
            ('2013-10-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_missing_deactivate_from_activation(self):
        inputs = [
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        # NO LONGER expect no event.
        # self._check_output(inputs, tuple())
        expected = (('2013-04-01T00:00:01.123457', DEACTIVATED, "activate => validate(inactive)",
                     '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_activate_from_validation(self):
        inputs = [
            ('2013-10-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-09-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation (4/1)
        ]
        expected = (('2013-04-01T00:00:01.123456', ACTIVATED, "start => deactivate",
                     '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456'),)
        self._check_output(inputs, expected)

    def test_missing_early_activate_from_validation(self):
        inputs = [
            ('2013-10-01T00:00:01.123456', VALIDATED, False, '2012-04-01T00:00:01.123456'),
            ('2013-09-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation (4/1/12)
        ]
        # expect no event.
        self._check_output(inputs, tuple())

    def test_missing_activate_after_validation(self):
        inputs = [
            ('2013-10-01T00:00:01.123456', DEACTIVATED, None, None),
            # missing activation
            ('2013-09-01T00:00:01.123456', VALIDATED, False, '2013-04-01T00:00:01.123456'),
            ('2013-08-01T00:00:01.123456', DEACTIVATED, None, None),
            ('2013-04-01T00:00:01.123456', ACTIVATED, None, None),
        ]
        expected = (('2013-09-01T00:00:01.123457', ACTIVATED, "validate(inactive) => deactivate",
                     '2013-09-01T00:00:01.123456', '2013-10-01T00:00:01.123456'),)
        self._check_output(inputs, expected)
