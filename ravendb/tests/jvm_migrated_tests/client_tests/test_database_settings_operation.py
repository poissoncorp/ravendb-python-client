from typing import Dict

from ravendb.serverwide.operations.common import DatabaseSettings

from ravendb import DocumentStore
from ravendb.documents.operations.server_misc import ToggleDatabasesStateOperation
from ravendb.serverwide.operations.configuration import GetDatabaseSettingsOperation, PutDatabaseSettingsOperation
from ravendb.tests.test_base import TestBase


class TestDatabaseSettingsOperation(TestBase):
    def test_check_if_configuration_settings_is_empty(self):
        self.check_if_values_got_saved(self.store, {})

    def test_change_single_setting_key_on_server(self):
        name = "Storage.PrefetchResetThresholdInGb"
        value = "10"

        settings = {name: value}
        self.put_configuration_settings(self.store, settings)
        self.check_if_values_got_saved(self.store, settings)

    def test_change_multiple_settings_keys_on_server(self):
        settings = {
            "Storage.PrefetchResetThresholdInGb": "10",
            "Storage.TimeToSyncAfterFlushInSec": "35",
            "Tombstones.CleanupIntervalInMin": "10",
        }

        self.put_configuration_settings(self.store, settings)
        self.check_if_values_got_saved(self.store, settings)

    @staticmethod
    def put_configuration_settings(store: DocumentStore, settings: Dict[str, str]) -> None:
        store.maintenance.send(PutDatabaseSettingsOperation(store.database, settings))
        store.maintenance.server.send(ToggleDatabasesStateOperation(store.database, True))
        store.maintenance.server.send(ToggleDatabasesStateOperation(store.database, False))

    def check_if_values_got_saved(self, store: DocumentStore, data: Dict[str, str]):
        settings = self.get_configuration_settings(store)
        for key, value in data.items():
            configuration_value = settings.settings.get(key)
            self.assertIsNotNone(configuration_value)
            self.assertEqual(configuration_value, value)

    def get_configuration_settings(self, store: DocumentStore) -> DatabaseSettings:
        settings = store.maintenance.send(GetDatabaseSettingsOperation(store.database))
        self.assertIsNotNone(settings)
        return settings
