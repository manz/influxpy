from unittest.case import TestCase

from influxpy.client import client_wrapper


class InfluxTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        client_wrapper.setup_test_database()

    @classmethod
    def tearDownClass(cls):
        client_wrapper.restore_test_database()

    def setUp(self):
        client_wrapper.create_database()

    def teardown(self):
        client_wrapper.drop_database()
