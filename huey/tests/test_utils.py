import calendar
import datetime
import os
import time
import unittest

from huey.utils import UTC
from huey.utils import normalize_time
from huey.utils import reraise_as
from huey.utils import to_timestamp


class MyException(Exception): pass


class TestReraiseAs(unittest.TestCase):
    def test_wrap_exception(self):
        def raise_keyerror():
            try:
                {}['huey']
            except KeyError as exc:
                reraise_as(MyException)

        self.assertRaises(MyException, raise_keyerror)
        try:
            raise_keyerror()
        except MyException as exc:
            self.assertEqual(str(exc), "KeyError: 'huey'")
        else:
            raise AssertionError('MyException not raised as expected.')


class FakePacific(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=-8)
    def tzname(self, dt):
        return 'US/Pacific'
    def dst(self, dt):
        return datetime.timedelta(0)


class TestNormalizeTime(unittest.TestCase):
    def setUp(self):
        self._orig_tz = os.environ.get('TZ')
        os.environ['TZ'] = 'US/Pacific'
        time.tzset()

    def tearDown(self):
        del os.environ['TZ']
        if self._orig_tz:
            os.environ['TZ'] = self._orig_tz
        time.tzset()

    def test_normalize_time(self):
        ts_local = datetime.datetime(2000, 1, 1, 12, 0, 0)  # Noon on Jan 1.
        ts_utc = ts_local + datetime.timedelta(hours=8)  # For fake tz.
        ts_inv = ts_local - datetime.timedelta(hours=8)

        # Naive datetime.

        # No conversion is applied, as we treat everything as local time.
        self.assertEqual(normalize_time(ts_local, utc=False), ts_local)

        # So we provided a naive timestamp from the localtime (us/pacific),
        # which is 8 hours behind UTC in January.
        self.assertEqual(normalize_time(ts_local, utc=True), ts_utc)

        # TZ-aware datetime in local timezone (Fake US/Pacific).

        # Here we provide a tz-aware timestamp from the localtime (us/pacific).
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=FakePacific())

        # No conversion, treated as local time.
        self.assertEqual(normalize_time(ts, utc=False), ts_local)

        # Converted to UTC according to rules from our fake tzinfo, +8 hours.
        self.assertEqual(normalize_time(ts, utc=True), ts_utc)

        # TZ-aware datetime in UTC timezone.

        # Here we provide a tz-aware timestamp using UTC timezone.
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=UTC())

        # Since we're specifying utc=False, we are dealing with localtimes
        # internally. The timestamp passed in is a tz-aware timestamp in UTC.
        # To convert to a naive localtime, we subtract 8 hours (since UTC is
        # 8 hours ahead of our local time).
        self.assertEqual(normalize_time(ts, utc=False), ts_inv)

        # When utc=True there's no change, since the timestamp is already UTC.
        self.assertEqual(normalize_time(ts, utc=True), ts_local)


class TestToTimestampUTC(unittest.TestCase):
    def test_to_timestamp_utc_mode_returns_correct_utc_timestamp(self):
        dt = datetime.datetime(2023, 6, 15, 12, 0, 0, 123456)

        ts_utc = to_timestamp(dt, utc=True)
        expected_utc = calendar.timegm(dt.utctimetuple()) + (dt.microsecond * 1e-6)

        self.assertEqual(ts_utc, expected_utc)

        dt_utc = datetime.datetime.fromtimestamp(ts_utc, datetime.timezone.utc)
        dt_utc_naive = dt_utc.replace(tzinfo=None)
        self.assertEqual(dt_utc_naive, dt)

    def test_to_timestamp_local_mode_backward_compatible(self):
        dt = datetime.datetime(2023, 6, 15, 12, 0, 0, 123456)

        ts_local = to_timestamp(dt, utc=False)
        ts_default = to_timestamp(dt)

        self.assertEqual(ts_local, ts_default)
        self.assertEqual(ts_local, dt.timestamp())

    def test_to_timestamp_utc_mode_correctly_interprets_naive_datetime_as_utc(self):
        dt = datetime.datetime(2023, 6, 15, 12, 0, 0, 123456)

        ts_utc = to_timestamp(dt, utc=True)

        dt_reconstructed = datetime.datetime.fromtimestamp(ts_utc, datetime.timezone.utc)
        dt_reconstructed_naive = dt_reconstructed.replace(tzinfo=None)

        self.assertEqual(dt_reconstructed_naive, dt)

    def test_to_timestamp_utc_vs_local_semantic_difference(self):
        dt = datetime.datetime(2023, 6, 15, 12, 0, 0)

        ts_utc = to_timestamp(dt, utc=True)
        ts_local = to_timestamp(dt, utc=False)

        expected_utc = calendar.timegm(dt.utctimetuple())
        self.assertEqual(ts_utc, expected_utc)

        dt_from_utc = datetime.datetime.fromtimestamp(ts_utc, datetime.timezone.utc)
        dt_from_local = datetime.datetime.fromtimestamp(ts_local, datetime.timezone.utc)

        self.assertEqual(dt_from_utc.replace(tzinfo=None), dt)

        if time.timezone != 0 or time.daylight:
            self.assertNotEqual(ts_utc, ts_local)
            self.assertNotEqual(dt_from_utc, dt_from_local)


class TestToTimestampDSTBoundary(unittest.TestCase):
    def test_dst_spring_forward_boundary(self):
        dt_before = datetime.datetime(2023, 3, 12, 1, 30, 0)
        dt_after = datetime.datetime(2023, 3, 12, 3, 30, 0)

        ts_before_utc = to_timestamp(dt_before, utc=True)
        ts_after_utc = to_timestamp(dt_after, utc=True)

        expected_before = calendar.timegm(dt_before.utctimetuple())
        expected_after = calendar.timegm(dt_after.utctimetuple())

        self.assertEqual(ts_before_utc, expected_before)
        self.assertEqual(ts_after_utc, expected_after)

        expected_diff = (dt_after - dt_before).total_seconds()
        actual_diff = ts_after_utc - ts_before_utc
        self.assertEqual(actual_diff, expected_diff)

        dt_before_reconstructed = datetime.datetime.fromtimestamp(
            ts_before_utc, datetime.timezone.utc).replace(tzinfo=None)
        dt_after_reconstructed = datetime.datetime.fromtimestamp(
            ts_after_utc, datetime.timezone.utc).replace(tzinfo=None)
        self.assertEqual(dt_before_reconstructed, dt_before)
        self.assertEqual(dt_after_reconstructed, dt_after)

    def test_dst_fall_back_boundary(self):
        dt_before = datetime.datetime(2023, 11, 5, 0, 30, 0)
        dt_after = datetime.datetime(2023, 11, 5, 2, 30, 0)

        ts_before_utc = to_timestamp(dt_before, utc=True)
        ts_after_utc = to_timestamp(dt_after, utc=True)

        expected_before = calendar.timegm(dt_before.utctimetuple())
        expected_after = calendar.timegm(dt_after.utctimetuple())

        self.assertEqual(ts_before_utc, expected_before)
        self.assertEqual(ts_after_utc, expected_after)

        expected_diff = (dt_after - dt_before).total_seconds()
        actual_diff = ts_after_utc - ts_before_utc
        self.assertEqual(actual_diff, expected_diff)

        dt_before_reconstructed = datetime.datetime.fromtimestamp(
            ts_before_utc, datetime.timezone.utc).replace(tzinfo=None)
        dt_after_reconstructed = datetime.datetime.fromtimestamp(
            ts_after_utc, datetime.timezone.utc).replace(tzinfo=None)
        self.assertEqual(dt_before_reconstructed, dt_before)
        self.assertEqual(dt_after_reconstructed, dt_after)

    def test_utc_mode_consistent_throughout_year(self):
        test_dates = [
            datetime.datetime(2023, 1, 1, 12, 0, 0),
            datetime.datetime(2023, 3, 12, 2, 0, 0),
            datetime.datetime(2023, 6, 21, 12, 0, 0),
            datetime.datetime(2023, 11, 5, 2, 0, 0),
            datetime.datetime(2023, 12, 25, 12, 0, 0),
        ]

        for dt in test_dates:
            ts_utc = to_timestamp(dt, utc=True)
            expected = calendar.timegm(dt.utctimetuple())
            self.assertEqual(ts_utc, expected)

            dt_reconstructed = datetime.datetime.fromtimestamp(
                ts_utc, datetime.timezone.utc).replace(tzinfo=None)
            self.assertEqual(dt_reconstructed, dt)
