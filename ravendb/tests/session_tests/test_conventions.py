from typing import Type, Any

from ravendb import DocumentStore
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestConventions(TestBase):
    def setUp(self):
        super().setUp()

    def test_custom_find_identity_property_name(self):
        store = DocumentStore(self.store.urls, self.store.database)

        class MyUniqueCompany(Company):
            def __init__(self, name: str = None, Id: str = None):
                super().__init__(Id=Id, name=name)
                self.special_identity_field: str = "MySpecialCompanies/unique"

        def _find_identity_property_name(object_type: Type[Any]) -> str:
            if object_type is MyUniqueCompany:
                return "special_identity_field"
            return "Id"

        store.conventions.find_identity_property_name = _find_identity_property_name
        store.initialize()

        with store.open_session() as session:
            session.store(MyUniqueCompany(name="Borpa Corp", Id="PerfectlyRelevantDocumentId/1"))
            session.store(Company(name="Poissoncorp", Id="companies/1"))
            session.save_changes()

        with store.open_session() as session:
            non_existing_company = session.load("PerfectlyRelevantDocumentId/1", MyUniqueCompany)
            custom_id_field_company = session.load("MySpecialCompanies/unique", MyUniqueCompany)
            regular_company = session.load("companies/1", Company)

            self.assertIsNone(non_existing_company)
            self.assertEqual("Borpa Corp", custom_id_field_company.name)
            self.assertEqual("Poissoncorp", regular_company.name)
