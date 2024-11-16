

import unittest
from object_path import ObjectPathProcessor

class TestObjectPathProcessor(unittest.TestCase):

    def test_parse_object_path(self):
        processor = ObjectPathProcessor()
        object_path = "s3://my-bucket/xxx/yyy/zzz/abc/id=123/month=2019-01-01/2019-01-19T10:31:18.818Z.gz"
        result = processor.parse_object_path(object_path)
        self.assertEqual(result['id'], '123')
        self.assertEqual(result['month'], '2019-01-01')

    def test_find_min_max_months(self):
        processor = ObjectPathProcessor()
        months_by_id = {
            '123': ['2019-01-01', '2019-02-01', '2019-03-01'],
            '333': ['2019-01-01', '2019-02-01', '2019-05-01'],
        }
        result = processor.find_min_max_months(months_by_id)
        self.assertEqual(result['123']['min_month'], '2019-01-01')
        self.assertEqual(result['123']['max_month'], '2019-03-01')
        self.assertEqual(result['333']['missing_months'], ['2019-03-01', '2019-04-01'])

    def test_write_to_json(self):
        processor = ObjectPathProcessor()
        data = {'123': {'min_month': '2019-01-01', 'max_month': '2019-03-01'}}
        processor.write_to_json(data, 'test_output.json')

unittest.main()