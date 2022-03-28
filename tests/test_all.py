import unittest
from unittest.mock import patch, _ANY, Mock
import appsinstalled_pb2
from memc_load import (insert_appsinstalled, parse_appsinstalled,
                       dot_rename, main, AppsInstalled)


class TestUnits(unittest.TestCase):

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

    @patch('os.rename')
    def test_dot_rename(self, mock):
        path = '/home/username/misc/12145678.tsv.gz'
        dot_rename(path)
        mock.assert_called_with(path, '/home/username/misc/.12145678.tsv.gz')

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

    @patch('logging.debug')
    def test_insert_appinstalled_general_dry(self, logging_mock):
        memc_mock = Mock()
        memc_mock._client.server = ('127.0.0.1', 33013)
        appsinstalled = AppsInstalled(
            'idfa', '1rfw452y52g2gq4g', 55.55, 42.42,
            [1423, 43, 567, 3, 7, 23]
        )
        result = insert_appsinstalled(memc_mock, appsinstalled, dry_run=True)
        self.assertTrue(result)
        logging_mock.assert_called_once()

    def test_insert_appinstalled_real(self):
        memc_mock = Mock()
        memc_mock._client.server = ('127.0.0.1', 33013)
        memc_mock.set.side_effect = lambda x, y: True
        appsinstalled = AppsInstalled(
            'idfa', '1rfw452y52g2gq4g', 55.55, 42.42,
            [1423, 43, 567, 3, 7, 23]
        )
        result = insert_appsinstalled(memc_mock, appsinstalled,
                                      dry_run=False)
        memc_mock.set.assert_called_once()
        memc_mock.set.assert_called_with('idfa:1rfw452y52g2gq4g', _ANY())
        self.assertTrue(result)

    @patch('memc_load.dot_rename')  # avoiding renaming fixture
    def test_main_logic(self, return_mock):
        options = Mock()
        options.maxworkers = 3
        options.idfa = "127.0.0.1:33013"
        options.gaid = "127.0.0.1:33014"
        options.adid = "127.0.0.1:33015"
        options.dvid = "127.0.0.1:33016"
        options.pattern = "tests/fixtures/*.tsv.gz"
        main(options)
        return_mock.assert_called()
