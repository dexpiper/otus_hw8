import unittest

import appsinstalled_pb2
from memc_load import parse_appsinstalled


class TestProtoBuff(unittest.TestCase):

    def test_protobaf_serializer(self):
        sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"  # noqa E501
        for line in sample.splitlines():
            dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
            apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
            lat, lon = float(lat), float(lon)
            ua = appsinstalled_pb2.UserApps()
            ua.lat = lat
            ua.lon = lon
            ua.apps.extend(apps)
            packed = ua.SerializeToString()
            unpacked = appsinstalled_pb2.UserApps()
            unpacked.ParseFromString(packed)
            self.assertEqual(ua, unpacked)

    def test_parse_appinstalled_general(self):
        with open('tests/fixtures/sample.tsv', 'r') as file:
            for line in file:
                with self.subTest(line=line):
                    line = line.strip()
                    appsinstalled = parse_appsinstalled(line)
                    self.assertTrue(appsinstalled)

    def test_parse_appinstalled_values(self):
        line = 'idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23'
        appsinstalled = parse_appsinstalled(line.strip())
        self.assertEqual(appsinstalled.dev_type, 'idfa')
        self.assertEqual(appsinstalled.dev_id, '1rfw452y52g2gq4g')
        self.assertEqual(appsinstalled.lat, 55.55)
        self.assertEqual(appsinstalled.lon, 42.42)
        self.assertEqual(
            appsinstalled.apps,
            [1423, 43, 567, 3, 7, 23]
        )

    def test_parse_appinstalled_consumes_errors(self):
        line_with_error = (
            'idfa\t1rfw452y52g2gq4g\t55.55\t!!ERROR!!\t1423,43,ERROR,3,7,23'
        )
        appsinstalled = parse_appsinstalled(line_with_error.strip())
        self.assertTrue(appsinstalled)

    def test_parse_appinstalled_return_none(self):
        err_lines = [
            'This is a short line without tabs',
            'Just\tsome\ttabs',
            '\t\t\t42    '
        ]
        for err_line in err_lines:
            with self.subTest(err_line=err_line):
                self.assertIsNone(
                    parse_appsinstalled(err_line.strip())
                )
