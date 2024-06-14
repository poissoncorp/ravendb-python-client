from ravendb import RevisionsConfiguration, RevisionsCollectionConfiguration
from ravendb.documents.operations.revisions import ConfigureRevisionsOperation
from ravendb.documents.operations.statistics import GetDetailedCollectionStatisticsOperation
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class TestRavenDB14881(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_get_detailed_collection_statistics(self):
        configuration = RevisionsConfiguration()
        configuration.collections = {}

        revisions_collection_configuration = RevisionsCollectionConfiguration()
        revisions_collection_configuration.disabled = False
        configuration.collections["Companies"] = revisions_collection_configuration

        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        # insert sample data
        with self.store.bulk_insert() as bulk_insert:
            for i in range(20):
                company = Company(Id=f"company/{i}", name=f"name{i}")
                bulk_insert.store(company)

        # get detailed collection statistics before we are going to change some data
        # right now there shouldn't be any revisions

        detailed_collection_statistics = self.store.maintenance.send(GetDetailedCollectionStatisticsOperation())

        self.assertEqual(20, detailed_collection_statistics.count_of_documents)
        self.assertEqual(0, detailed_collection_statistics.count_of_conflicts)

        self.assertEqual(1, len(detailed_collection_statistics.collections))

        companies = detailed_collection_statistics.collections["Companies"]
        self.assertIsNotNone(companies)

        self.assertEqual(20, companies.count_of_documents)
        self.assertGreater(companies.size.size_in_bytes, 0)
        self.assertGreater(companies.documents_size.size_in_bytes, 0)
        self.assertGreater(companies.revisions_size.size_in_bytes, 0)

        self.assertEqual(
            companies.documents_size.size_in_bytes
            + companies.revisions_size.size_in_bytes
            + companies.tombstones_size.size_in_bytes,
            companies.size.size_in_bytes,
        )
