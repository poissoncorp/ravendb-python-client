from datetime import datetime, timedelta

from ravendb.documents.operations.time_series import (
    GetTimeSeriesOperation,
    TimeSeriesOperation,
    TimeSeriesBatchOperation,
    GetTimeSeriesStatisticsOperation,
    GetMultipleTimeSeriesOperation,
    TimeSeriesDetails,
)
from ravendb.documents.session.time_series import TimeSeriesRange
from ravendb.tests.test_base import TestBase, User


class TestTimeSeriesOperations(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_delete_without_providing_from_and_to_dates(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), doc_id)

            tsf = session.time_series_for(doc_id, "HeartRate")
            tsf2 = session.time_series_for(doc_id, "BloodPressure")
            tsf3 = session.time_series_for(doc_id, "BodyTemperature")

            for j in range(100):
                tsf.append_single(base_line + timedelta(minutes=j), j)
                tsf2.append_single(base_line + timedelta(minutes=j), j)
                tsf3.append_single(base_line + timedelta(minutes=j), j)

            session.save_changes()

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "HeartRate"))
        self.assertEqual(100, len(get.entries))

        # null From, To
        delete_op = TimeSeriesOperation()
        delete_op.name = "Heartrate"
        delete_op.delete(TimeSeriesOperation.DeleteOperation())

        self.store.operations.send(TimeSeriesBatchOperation(doc_id, delete_op))

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "HeartRate"))

        self.assertIsNone(get)

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BloodPressure"))
        self.assertEqual(100, len(get.entries))

        # null to

        delete_op = TimeSeriesOperation("BloodPressure")
        delete_op.delete(TimeSeriesOperation.DeleteOperation(base_line + timedelta(minutes=50), None))

        self.store.operations.send(TimeSeriesBatchOperation(doc_id, delete_op))
        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BloodPressure"))
        self.assertEqual(50, len(get.entries))

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BodyTemperature"))
        self.assertEqual(100, len(get.entries))

        # null From
        delete_op = TimeSeriesOperation("BodyTemperature")
        delete_op.delete(TimeSeriesOperation.DeleteOperation(None, base_line + timedelta(minutes=19)))

        self.store.operations.send(TimeSeriesBatchOperation(doc_id, delete_op))

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BodyTemperature"))
        self.assertEqual(80, len(get.entries))

    def test_can_delete_timestamps_using_store_operations(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        time_series_op = TimeSeriesOperation("Heartrate")

        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=1), [59], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=2), [61], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=3), [60], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=4), [62.5], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=5), [62], "watches/fitbit")
        )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation(document_id, "Heartrate"))

        self.assertEqual(5, len(time_series_range_result.entries))

        time_series_op = TimeSeriesOperation("Heartrate")
        time_series_op.delete(
            TimeSeriesOperation.DeleteOperation(base_line + timedelta(seconds=2), base_line + timedelta(seconds=3))
        )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation(document_id, "Heartrate"))

        self.assertEqual(3, len(time_series_range_result.entries))

        value = time_series_range_result.entries[0]
        self.assertEqual(59, value.values[0])
        self.assertEqual(base_line + timedelta(seconds=1), value.timestamp)

        value = time_series_range_result.entries[1]
        self.assertEqual(62.5, value.values[0])
        self.assertEqual(base_line + timedelta(seconds=4), value.timestamp)

        value = time_series_range_result.entries[2]
        self.assertEqual(62, value.values[0])
        self.assertEqual(base_line + timedelta(seconds=5), value.timestamp)

        with self.store.open_session() as session:
            session.delete(document_id)
            session.save_changes()

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.save_changes()

            tsf = session.time_series_for(document_id, "Heartrate")
            tsf.append(base_line + timedelta(minutes=1), [59], "watches/fitbit")
            tsf.append(base_line + timedelta(minutes=2), [69], "watches/fitbit")
            tsf.append(base_line + timedelta(minutes=3), [79], "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.time_series_for(document_id, "Heartrate").delete_at(base_line + timedelta(minutes=2))
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get()

            self.assertEqual(2, len(vals))

            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([79], vals[1].values)
            self.assertEqual("watches/fitbit", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[1].timestamp)

    def test_get_time_series_statistics(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User()
            session.store(user, document_id)

            ts = session.time_series_for(document_id, "heartrate")
            for i in range(11):
                ts.append_single(base_line + timedelta(minutes=i * 10), 72, "watches/fitbit")

            ts = session.time_series_for(document_id, "pressure")
            for i in range(10, 21):
                ts.append_single(base_line + timedelta(minutes=i * 10), 72, "watches/fitbit")

            session.save_changes()

        op = self.store.operations.send(GetTimeSeriesStatisticsOperation(document_id))
        self.assertEqual(document_id, op.document_id)
        self.assertEqual(2, len(op.time_series))

        ts1 = op.time_series[0]
        ts2 = op.time_series[1]

        self.assertEqual("heartrate", ts1.name)
        self.assertEqual("pressure", ts2.name)

        self.assertEqual(11, ts1.number_of_entries)
        self.assertEqual(11, ts2.number_of_entries)

        self.assertEqual(base_line, ts1.start_date)
        self.assertEqual(base_line + timedelta(minutes=10 * 10), ts1.end_date)

        self.assertEqual(base_line + timedelta(minutes=10 * 10), ts2.start_date)
        self.assertEqual(base_line + timedelta(minutes=20 * 10), ts2.end_date)

    def test_can_get_multiple_ranges_in_single_request(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        time_series_op = TimeSeriesOperation("Heartrate")
        for i in range(361):
            time_series_op.append(
                TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=i * 10), [59], "watches/fitbit")
            )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)
        self.store.operations.send(time_series_batch)

        time_series_details: TimeSeriesDetails = self.store.operations.send(
            GetMultipleTimeSeriesOperation(
                document_id,
                [
                    TimeSeriesRange("Heartrate", base_line + timedelta(minutes=5), base_line + timedelta(minutes=10)),
                    TimeSeriesRange("Heartrate", base_line + timedelta(minutes=15), base_line + timedelta(minutes=30)),
                    TimeSeriesRange("Heartrate", base_line + timedelta(minutes=40), base_line + timedelta(minutes=60)),
                ],
            )
        )

        self.assertEqual(document_id, time_series_details.key)
        self.assertEqual(1, len(time_series_details.values))
        self.assertEqual(3, len(time_series_details.values.get("Heartrate")))

        range_ = time_series_details.values.get("Heartrate")[0]

        self.assertEqual(base_line + timedelta(minutes=5), range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=10), range_.to_date)

        self.assertEqual(31, len(range_.entries))
        self.assertEqual(base_line + timedelta(minutes=5), range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=10), range_.entries[30].timestamp)

        range_ = time_series_details.values.get("Heartrate")[1]

        self.assertEqual(base_line + timedelta(minutes=15), range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=30), range_.to_date)

        self.assertEqual(91, len(range_.entries))
        self.assertEqual(base_line + timedelta(minutes=15), range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=30), range_.entries[90].timestamp)

        range_ = time_series_details.values.get("Heartrate")[2]

        self.assertEqual(base_line + timedelta(minutes=40), range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=60), range_.to_date)

        self.assertEqual(121, len(range_.entries))
        self.assertEqual(base_line + timedelta(minutes=40), range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=60), range_.entries[120].timestamp)

    def test_can_get_multiple_time_series_in_single_request(self):
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        # append

        base_line = datetime(2023, 8, 20, 21, 30)

        time_series_op = TimeSeriesOperation("Heartrate")

        for i in range(11):
            time_series_op.append(
                TimeSeriesOperation.AppendOperation(base_line + timedelta(minutes=i * 10), [72], "watches/fitbit")
            )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        time_series_op = TimeSeriesOperation("BloodPressure")

        for i in range(11):
            time_series_op.append(TimeSeriesOperation.AppendOperation(base_line + timedelta(minutes=i * 10), [80]))

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)
        self.store.operations.send(time_series_batch)

        time_series_op = TimeSeriesOperation("Temperature")

        for i in range(11):
            time_series_op.append(
                TimeSeriesOperation.AppendOperation(base_line + timedelta(minutes=i * 10), [37 + i * 0.15])
            )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)
        self.store.operations.send(time_series_batch)

        # get ranges from multiple time series in a single request

        time_series_details: TimeSeriesDetails = self.store.operations.send(
            GetMultipleTimeSeriesOperation(
                document_id,
                [
                    TimeSeriesRange("Heartrate", base_line, base_line + timedelta(minutes=15)),
                    TimeSeriesRange("Heartrate", base_line + timedelta(minutes=30), base_line + timedelta(minutes=45)),
                    TimeSeriesRange("BloodPressure", base_line, base_line + timedelta(minutes=30)),
                    TimeSeriesRange(
                        "BloodPressure", base_line + timedelta(minutes=60), base_line + timedelta(minutes=90)
                    ),
                    TimeSeriesRange("Temperature", base_line, base_line + timedelta(days=1)),
                ],
            )
        )

        self.assertEqual(document_id, time_series_details.key)
        self.assertEqual(3, len(time_series_details.values))

        self.assertEqual(2, len(time_series_details.values["Heartrate"]))

        range_ = time_series_details.values.get("Heartrate")[0]

        self.assertEqual(base_line, range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=15), range_.to_date)

        self.assertEqual(2, len(range_.entries))

        self.assertEqual(base_line, range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=10), range_.entries[1].timestamp)

        self.assertIsNone(range_.total_results)

        range_ = time_series_details.values.get("Heartrate")[1]

        self.assertEqual(base_line + timedelta(minutes=30), range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=45), range_.to_date)

        self.assertEqual(2, len(range_.entries))
        self.assertEqual(base_line + timedelta(minutes=30), range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=40), range_.entries[1].timestamp)

        self.assertIsNone(range_.total_results)

        range_ = time_series_details.values.get("BloodPressure")[0]
        self.assertEqual(base_line, range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=30), range_.to_date)

        self.assertEqual(4, len(range_.entries))

        self.assertEqual(base_line, range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=30), range_.entries[3].timestamp)

        self.assertIsNone(range_.total_results)

        range_ = time_series_details.values.get("BloodPressure")[1]

        self.assertEqual(base_line + timedelta(minutes=60), range_.from_date)
        self.assertEqual(base_line + timedelta(minutes=90), range_.to_date)

        self.assertEqual(4, len(range_.entries))

        self.assertEqual(base_line + timedelta(minutes=60), range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=90), range_.entries[3].timestamp)

        self.assertIsNone(range_.total_results)

        self.assertEqual(1, len(time_series_details.values.get("Temperature")))

        range_ = time_series_details.values.get("Temperature")[0]

        self.assertEqual(base_line, range_.from_date)
        self.assertEqual(base_line + timedelta(days=1), range_.to_date)

        self.assertEqual(11, len(range_.entries))

        self.assertEqual(base_line, range_.entries[0].timestamp)
        self.assertEqual(base_line + timedelta(minutes=100), range_.entries[10].timestamp)

        self.assertEqual(11, range_.total_results)  # full range

    def test_get_multiple_time_series_should_throw_on_missing_name_from_range(self):
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        base_line = datetime(2023, 8, 20, 21, 30)

        time_series_op = TimeSeriesOperation("Heartrate")

        for i in range(11):
            time_series_op.append(
                TimeSeriesOperation.AppendOperation(base_line + timedelta(minutes=i * 10), [72], "watches/fitbit")
            )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        self.assertRaisesWithMessage(
            self.store.operations.send,
            ValueError,
            "Missing name argument in TimeSeriesRange. Name cannot be None or empty",
            GetMultipleTimeSeriesOperation(document_id, [TimeSeriesRange(None, base_line, None)]),
        )

    def test_should_throw_on_null_or_empty_ranges(self):
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        base_line = datetime(2023, 8, 20, 21, 30)

        time_series_op = TimeSeriesOperation("Heartrate")

        for i in range(11):
            time_series_op.append(
                TimeSeriesOperation.AppendOperation(base_line + timedelta(minutes=i * 10), [72], "watches/fitbit")
            )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        with self.assertRaises(ValueError):
            self.store.operations.send(GetTimeSeriesOperation(document_id, None))

        with self.assertRaises(ValueError):
            self.store.operations.send(GetMultipleTimeSeriesOperation(document_id, []))

    def test_get_time_series_should_throw_on_missing_name(self):
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        base_line = datetime(2023, 8, 20, 21, 30)

        time_series_op = TimeSeriesOperation("Heartrate")

        for i in range(11):
            time_series_op.append(
                TimeSeriesOperation.AppendOperation(base_line + timedelta(minutes=i * 10), [72], "watches/fitbit")
            )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)
        failed = False
        try:
            GetTimeSeriesOperation(document_id, "", base_line, base_line + timedelta(days=3650))
        except ValueError as ex:
            failed = True
            self.assertIn(ex.args[0], "Timeseries cannot be None or empty")

        self.assertTrue(failed)
