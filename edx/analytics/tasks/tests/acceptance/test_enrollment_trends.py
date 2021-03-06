"""
End to end test of enrollment trends.
"""

import datetime
import logging

from luigi.s3 import S3Target

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class EnrollmentTrendsAcceptanceTest(AcceptanceTestCase):

    INPUT_FILE = 'enrollment_trends_tracking.log'

    def setUp(self):
        super(EnrollmentTrendsAcceptanceTest, self).setUp()

        assert 'oddjob_jar' in self.config

        self.oddjob_jar = self.config['oddjob_jar']

    def test_enrollment_trends(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 8, 1))

        blacklist_path = url_path_join(self.test_src, 'blacklist')
        blacklist_date = '2014-08-29'
        blacklist_url = url_path_join(blacklist_path, 'dt=' + blacklist_date, 'blacklist.tsv')
        with S3Target(blacklist_url).open('w') as f:
            f.write('edX/Open_DemoX/edx_demo_course3')

        config_override = {
            'enrollments': {
                'blacklist_date': blacklist_date,
                'blacklist_path': blacklist_path,
            }
        }

        self.task.launch([
            'ImportCourseDailyFactsIntoMysql',
            '--credentials', self.export_db.credentials_file_url,
            '--src', self.test_src,
            '--dest', self.test_out,
            '--name', 'test',
            '--include', '"*"',
            '--run-date', '2014-08-06',
            '--manifest', url_path_join(self.test_root, 'manifest.txt'),
            '--lib-jar', self.oddjob_jar,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ], config_override=config_override)

        self.validate_output()

    def validate_output(self):
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, count FROM course_enrollment_daily ORDER BY date ASC')
            results = cursor.fetchall()

        self.maxDiff = None

        self.assertItemsEqual(results, [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 3),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 3),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 2),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 2),
        ])
