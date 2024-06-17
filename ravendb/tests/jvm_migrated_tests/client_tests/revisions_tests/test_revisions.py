import unittest
from datetime import datetime
from time import sleep

from ravendb import RevisionsConfiguration, RevisionsCollectionConfiguration, GetStatisticsOperation
from ravendb.documents.commands.revisions import GetRevisionsBinEntryCommand
from ravendb.documents.operations.revisions import ConfigureRevisionsOperation, GetRevisionsOperation
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company
from ravendb.primitives import constants
from ravendb.tests.test_base import TestBase


class TestRevisions(TestBase):
    def setUp(self):
        super().setUp()

    def test_revisions(self):
        TestBase.setup_revisions(self.store, False, 4)

        for i in range(4):
            with self.store.open_session() as session:
                user = User(name=f"user{i+1}")
                session.store(user, "users/1")
                session.save_changes()

        with self.store.open_session() as session:
            all_revisions = session.advanced.revisions.get_for("users/1", User)
            self.assertEqual(4, len(all_revisions))
            self.assertEqual(["user4", "user3", "user2", "user1"], [x.name for x in all_revisions])

            revisions_skip_first = session.advanced.revisions.get_for("users/1", User, 1)
            self.assertEqual(3, len(revisions_skip_first))
            self.assertEqual(["user3", "user2", "user1"], [x.name for x in revisions_skip_first])

            revisions_skip_first_take_two = session.advanced.revisions.get_for("users/1", User, 1, 2)
            self.assertEqual(2, len(revisions_skip_first_take_two))
            self.assertEqual(["user3", "user2"], [x.name for x in revisions_skip_first_take_two])

            all_metadata = session.advanced.revisions.get_metadata_for("users/1")
            self.assertEqual(4, len(all_metadata))

            metadata_skip_first = session.advanced.revisions.get_metadata_for("users/1", 1)
            self.assertEqual(3, len(metadata_skip_first))

            metadata_skip_first_take_two = session.advanced.revisions.get_metadata_for("users/1", 1, 2)
            self.assertEqual(2, len(metadata_skip_first_take_two))

    def test_can_get_revisions_by_change_vector(self):
        id_ = "users/1"
        self.setup_revisions(self.store, False, 100)

        with self.store.open_session() as session:
            user = User()
            user.name = "Fitzchak"
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, User)
                user.name = f"Fitzchak{i}"
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

            change_vectors = [x[constants.Documents.Metadata.CHANGE_VECTOR] for x in revisions_metadata]
            change_vectors.append("NotExistsChangeVector")

            revisions = session.advanced.revisions.get_by_change_vectors(change_vectors, User)
            self.assertIsNone(revisions.get("NotExistsChangeVector"))
            self.assertIsNone(session.advanced.revisions.get_by_change_vector("NotExistsChangeVector", User))

    def test_can_get_revisions_count_for(self):
        company = Company()
        company.name = "Company Name"

        self.setup_revisions(self.store, False, 100)
        with self.store.open_session() as session:
            session.store(company)
            session.save_changes()

        with self.store.open_session() as session:
            company2 = session.load(company.Id, Company)
            company2.fax = "Israel"
            session.save_changes()

        with self.store.open_session() as session:
            company3 = session.load(company.Id, Company)
            company3.name = "Hibernating Rhinos"
            session.save_changes()

        with self.store.open_session() as session:
            companies_revisions_count = session.advanced.revisions.get_count_for(company.Id)
            self.assertEqual(3, companies_revisions_count)

    def test_can_list_revisions_bin(self):
        self.setup_revisions(self.store, False, 4)

        with self.store.open_session() as session:
            user = User(name="user1")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.delete("users/1")
            session.save_changes()

        revisions_bin_entry_command = GetRevisionsBinEntryCommand(0, 20)
        self.store.get_request_executor().execute_command(revisions_bin_entry_command)

        result = revisions_bin_entry_command.result
        self.assertEqual(1, len(result.results))

        self.assertEqual("users/1", result.results[0].get("@metadata").get("@id"))

    def test_collection_case_sensitive_test_1(self):
        id_ = "user/1"
        configuration = RevisionsConfiguration()

        collection_configuration = RevisionsCollectionConfiguration()
        collection_configuration.disabled = False

        configuration.collections = {"uSErs": collection_configuration}

        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        with self.store.open_session() as session:
            user = User(name="raven")
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, User)
                user.name = "raven" + str(i)
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

    def test_collection_case_sensitive_test_2(self):
        id_ = "uSEr/1"
        configuration = RevisionsConfiguration()

        collection_configuration = RevisionsCollectionConfiguration()
        collection_configuration.disabled = False

        configuration.collections = {"users": collection_configuration}
        self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

        with self.store.open_session() as session:
            user = User(name="raven")
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, User)
                user.name = "raven" + str(i)
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

    def test_collection_case_sensitive_test_3(self):
        configuration = RevisionsConfiguration()
        c1 = RevisionsCollectionConfiguration()
        c1.disabled = False

        c2 = RevisionsCollectionConfiguration()
        c2.disabled = False

        configuration.collections = {"users": c1, "USERS": c2}
        with self.assertRaises(RuntimeError):
            self.store.maintenance.send(ConfigureRevisionsOperation(configuration))

    def test_can_get_all_revisions_for_document_using_store_operation(self):
        company = Company(name="Company Name")
        self.setup_revisions(self.store, False, 123)

        with self.store.open_session() as session:
            session.store(company)
            session.save_changes()

        with self.store.open_session() as session:
            company3 = session.load(company.Id, Company)
            company3.name = "Hibernating Rhinos"
            session.save_changes()

        revisions_result = self.store.operations.send(GetRevisionsOperation(company.Id, Company))
        self.assertEqual(2, revisions_result.total_results)

        companies_revisions = revisions_result.results
        self.assertEqual(2, len(companies_revisions))
        self.assertEqual("Hibernating Rhinos", companies_revisions[0].name)
        self.assertEqual("Company Name", companies_revisions[1].name)

    def test_can_get_revisions_with_paging_using_store_operation(self):
        self.setup_revisions(self.store, False, 123)

        id_ = "companies/1"

        with self.store.open_session() as session:
            session.store(Company(), id_)
            session.save_changes()

        with self.store.open_session() as session:
            company2 = session.load(id_, Company)
            company2.name = "Hibernating"
            session.save_changes()

        with self.store.open_session() as session:
            company3 = session.load(id_, Company)
            company3.name = "Hibernating Rhinos"
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                company = session.load(id_, Company)
                company.name = f"HR{i}"
                session.save_changes()

        parameters = GetRevisionsOperation.Parameters()
        parameters.id_ = id_
        parameters.start = 10
        revisions_result = self.store.operations.send(GetRevisionsOperation.from_parameters(parameters, Company))

        self.assertEqual(13, revisions_result.total_results)

        companies_revisions = revisions_result.results
        self.assertEqual(3, len(companies_revisions))

        self.assertEqual("Hibernating Rhinos", companies_revisions[0].name)
        self.assertEqual("Hibernating", companies_revisions[1].name)
        self.assertIsNone(companies_revisions[2].name)

    def test_can_get_revisions_with_paging2_using_store_operation(self):
        self.setup_revisions(self.store, False, 100)
        id_ = "companies/1"

        with self.store.open_session() as session:
            session.store(Company(), id_)
            session.save_changes()

        for i in range(99):
            with self.store.open_session() as session:
                company = session.load(id_, Company)
                company.name = "HR" + str(i)
                session.save_changes()

        revisions_result = self.store.operations.send(GetRevisionsOperation(id_, Company, 50, 10))

        self.assertEqual(100, revisions_result.total_results)

        companies_revisions = revisions_result.results
        self.assertEqual(10, len(companies_revisions))

        count = 0
        for i in range(48, 38, -1):
            self.assertEqual("HR" + str(i), companies_revisions[count].name)
            count += 1

    def test_can_get_metadata_for_lazily(self):
        id_ = "users/1"
        id_2 = "users/2"

        self.setup_revisions(self.store, False, 100)

        with self.store.open_session() as session:
            user1 = User()
            user1.name = "Omer"
            session.store(user1, id_)

            user2 = User()
            user2.name = "Rhinos"
            session.store(user2, id_2)

            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, User)
                user.name = f"Omer{i}"
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            revisions_metadata_lazily = session.advanced.revisions.lazily.get_metadata_for(id_)
            revisions_metadata_lazily_2 = session.advanced.revisions.lazily.get_metadata_for(id_2)
            revisions_metadata_lazily_result = revisions_metadata_lazily.value

            ids = [x["@id"] for x in revisions_metadata]
            ids_lazily = [x["@id"] for x in revisions_metadata_lazily_result]

            self.assertEqual(ids, ids_lazily)
            self.assertEqual(2, session.advanced.number_of_requests)

    def test_can_get_for_lazily(self):
        id_ = "users/1"
        id_2 = "users/2"

        self.setup_revisions(self.store, False, 100)

        with self.store.open_session() as session:
            user1 = User()
            user1.name = "Omer"
            session.store(user1, id_)

            user2 = User()
            user2.name = "Rhinos"
            session.store(user2, id_2)

            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, User)
                user.name = f"Omer{i}"
                session.save_changes()

        with self.store.open_session() as session:
            revision = session.advanced.revisions.get_for("users/1", User)
            revisions_lazily = session.advanced.revisions.lazily.get_for("users/1", User)
            session.advanced.revisions.lazily.get_for("users/2", User)

            revisions_lazily_result = revisions_lazily.value

            names = [x.name for x in revision]
            names_lazily = [x.name for x in revisions_lazily_result]
            self.assertEqual(names, names_lazily)

            ids = [x.Id for x in revision]
            ids_lazily = [x.Id for x in revisions_lazily_result]
            self.assertEqual(ids, ids_lazily)

    def test_can_get_revisions_by_id_and_time_lazily(self):
        id_ = "users/1"
        id_2 = "users/2"

        self.setup_revisions(self.store, False, 100)

        with self.store.open_session() as session:
            user1 = User()
            user1.name = "Omer"
            session.store(user1, id_)

            user2 = User()
            user2.name = "Rhinos"
            session.store(user2, id_2)

            session.save_changes()

        with self.store.open_session() as session:
            revision = session.advanced.lazily.load("users/1", User)
            doc = revision.value
            self.assertEqual(1, session.advanced.number_of_requests)

        with self.store.open_session() as session:
            revision = session.advanced.revisions.get_by_before_date("users/1", datetime.utcnow(), User)
            revisions_lazily = session.advanced.revisions.lazily.get_by_before_date("users/1", datetime.utcnow(), User)
            session.advanced.revisions.lazily.get_by_before_date("users/2", datetime.utcnow(), User)

            revisions_lazily_result = revisions_lazily.value

            self.assertEqual(revisions_lazily_result.Id, revision.Id)
            self.assertEqual(revisions_lazily_result.name, revision.name)
            self.assertEqual(2, session.advanced.number_of_requests)

    def test_can_get_non_existing_revisions_by_change_vector_async_lazily(self):
        with self.store.open_session() as session:
            lazy = session.advanced.revisions.lazily.get_by_change_vector("dummy", User)
            user = lazy.value

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertIsNone(user)

    def test_can_get_revisions_by_change_vectors_lazily(self):
        id_ = "users/1"

        self.setup_revisions(self.store, False, 123)

        with self.store.open_session() as session:
            user = User()
            user.name = "Omer"
            session.store(user, id_)
            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, User)
                user.name = f"Omer{i}"
                session.save_changes()

        with self.store.open_session() as session:
            revisions_metadata = session.advanced.revisions.get_metadata_for(id_)
            self.assertEqual(11, len(revisions_metadata))

            change_vectors = [x[constants.Documents.Metadata.CHANGE_VECTOR] for x in revisions_metadata]
            change_vectors2 = [x[constants.Documents.Metadata.CHANGE_VECTOR] for x in revisions_metadata]

            revisions_lazy = session.advanced.revisions.lazily.get_by_change_vectors(change_vectors, User)
            revisions_lazy2 = session.advanced.revisions.lazily.get_by_change_vectors(change_vectors2, User)

            lazy_result = revisions_lazy.value
            revisions = session.advanced.revisions.get_by_change_vectors(change_vectors, User)

            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertEqual(revisions.keys(), lazy_result.keys())

    @unittest.skip("RDBC-779 Flaky test")
    def test_can_get_revisions_by_change_vector_lazily(self):
        id_ = "users/1"
        id_2 = "users/2"

        self.setup_revisions(self.store, False, 123)

        with self.store.open_session() as session:
            user1 = User()
            user1.name = "Omer"
            session.store(user1, id_)

            user2 = User()
            user2.name = "Rhinos"
            session.store(user2, id_2)

            session.save_changes()

        for i in range(10):
            with self.store.open_session() as session:
                user = session.load(id_, Company)
                user.name = f"Omer{i}"
                session.save_changes()

        stats = self.store.maintenance.send(GetStatisticsOperation())
        db_id = stats.database_id

        cv = f"A:23-{db_id}"
        cv2 = f"A:3-{db_id}"

        with self.store.open_session() as session:
            sleep(0.33)
            revisions = session.advanced.revisions.get_by_change_vector(cv, User)
            revisions_lazily = session.advanced.revisions.lazily.get_by_change_vector(cv, User)
            revisions_lazily1 = session.advanced.revisions.lazily.get_by_change_vector(cv2, User)

            sleep(0.33)
            revisions_lazily_value = revisions_lazily.value

            sleep(0.33)
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertEqual(revisions.Id, revisions_lazily_value.Id)
            self.assertEqual(revisions.name, revisions_lazily_value.name)

        with self.store.open_session() as session:
            revisions = session.advanced.revisions.get_by_change_vector(cv, User)
            revisions_lazily = session.advanced.revisions.lazily.get_by_change_vector(cv, User)
            revisions_lazily_value = revisions_lazily.value

            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertEqual(revisions.Id, revisions_lazily_value.Id)
            self.assertEqual(revisions.name, revisions_lazily_value.name)
