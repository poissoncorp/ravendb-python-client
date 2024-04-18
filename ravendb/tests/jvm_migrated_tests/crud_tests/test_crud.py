from ravendb import DocumentsChanges
from ravendb.tests.test_base import TestBase, User


class Poc:
    def __init__(self, name, user):
        self.name = name
        self.user = user


class Member:
    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age


class Family:
    def __init__(self, names):
        self.names = names


class FamilyMembers:
    def __init__(self, members):
        self.members = members


class Arr1:
    def __init__(self, strings=None):
        self.strings = strings


class Arr2:
    def __init__(self, arr1=None):
        self.arr1 = arr1


class TestCrud(TestBase):
    def setUp(self):
        super(TestCrud, self).setUp()

    def test_update_property_from_null_to_object(self):
        poc = Poc("jacek", None)

        with self.store.open_session() as session:
            session.store(poc, "pocs/1")
            session.save_changes()

        with self.store.open_session() as session:
            poc = session.load("pocs/1", Poc)
            self.assertIsNone(poc.user)

            poc.user = User(None, None)
            session.save_changes()

        with self.store.open_session() as session:
            poc = session.load("pocs/1", Poc)
            self.assertIsNotNone(poc)

    def test_crud_operations_with_array_of_objects(self):
        with self.store.open_session() as session:
            member1 = Member("Hibernating Rhinos", 8)
            member2 = Member("RavenDB", 4)
            family = FamilyMembers([member1, member2])
            session.store(family, "family/1")
            session.save_changes()

            member1 = Member("RavenDB", 4)
            member2 = Member("Hibernating Rhinos", 8)
            new_family = session.load("family/1", FamilyMembers)
            new_family.members = [member1, member2]

            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual(4, len(changes["family/1"]))

            #  just dealing with async results to make proper assertion
            changes = sorted(changes["family/1"], key=lambda change: (change.field_name, change.field_old_value))

            self.assertEqual("age", changes[0].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[0].change)
            self.assertEqual(4, changes[0].field_old_value)
            self.assertEqual(8, changes[0].field_new_value)

            self.assertEqual("age", changes[1].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[1].change)
            self.assertEqual(8, changes[1].field_old_value)
            self.assertEqual(4, changes[1].field_new_value)

            self.assertEqual("name", changes[2].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[2].change)
            self.assertEqual("Hibernating Rhinos", changes[2].field_old_value)
            self.assertEqual("RavenDB", changes[2].field_new_value)

            self.assertEqual("name", changes[3].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[3].change)
            self.assertEqual("RavenDB", changes[3].field_old_value)
            self.assertEqual("Hibernating Rhinos", changes[3].field_new_value)

            member1 = Member("Toli", 5)
            member2 = Member("Boki", 15)
            new_family.members = [member1, member2]

            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            self.assertEqual(4, len(changes["family/1"]))

            changes = sorted(changes["family/1"], key=lambda change: (change.field_name, change.field_old_value))

            self.assertEqual("age", changes[0].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[0].change)
            self.assertEqual(4, changes[0].field_old_value)
            self.assertEqual(15, changes[0].field_new_value)

            self.assertEqual("age", changes[1].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[1].change)
            self.assertEqual(8, changes[1].field_old_value)
            self.assertEqual(5, changes[1].field_new_value)

            self.assertEqual("name", changes[2].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[2].change)
            self.assertEqual("Hibernating Rhinos", changes[2].field_old_value)
            self.assertEqual("Toli", changes[2].field_new_value)

            self.assertEqual("name", changes[3].field_name)
            self.assertEqual(DocumentsChanges.ChangeType.FIELD_CHANGED, changes[3].change)
            self.assertEqual("RavenDB", changes[3].field_old_value)
            self.assertEqual("Boki", changes[3].field_new_value)

    def test_crud_operations_with_array_in_object(self):
        with self.store.open_session() as session:
            family = Family(["Hibernating Rhinos", "RavenDB"])
            session.store(family, "family/1")
            session.save_changes()

            new_family = session.load("family/1", Family)
            new_family.names = ["Toli", "Mitzi", "Boki"]
            self.assertEqual(1, len(session.advanced.what_changed()))
            session.save_changes()

    def test_crud_operations_with_array_in_object_2(self):
        with self.store.open_session() as session:
            family = Family(["Hibernating Rhinos", "RavenDB"])
            session.store(family, "family/1")
            session.save_changes()

            new_family = session.load("family/1", Family)
            new_family.names = ["Hibernating Rhinos", "RavenDB"]
            self.assertEqual(0, len(session.advanced.what_changed()))
            new_family.names = ["RavenDB", "Hibernating Rhinos"]
            self.assertEqual(1, len(session.advanced.what_changed()))

    def test_crud_operations_with_array_in_object_3(self):
        with self.store.open_session() as session:
            family = Family(["Hibernating Rhinos", "RavenDB"])
            session.store(family, "family/1")
            session.save_changes()

            new_family = session.load("family/1", Family)
            new_family.names = ["RavenDB"]
            self.assertEqual(1, len(session.advanced.what_changed()))
            session.save_changes()

    def test_crud_operations_with_array_in_object_4(self):
        with self.store.open_session() as session:
            family = Family(["Hibernating Rhinos", "RavenDB"])
            session.store(family, "family/1")
            session.save_changes()

            new_family = session.load("family/1", Family)
            new_family.names = ["RavenDB", "Hibernating Rhinos", "Toli", "Mitzi", "Boki"]
            self.assertEqual(1, len(session.advanced.what_changed()))
            session.save_changes()

    def test_crud_operations_with_array_in_object_5(self):
        with self.store.open_session() as session:
            family = Family(["Gracjan", "OG IT"])
            session.store(family, "family/gracjan")
            session.save_changes()

        with self.store.open_session() as session:
            new_family = session.load("family/gracjan", Family)
            new_family.names = ["Gracjan"]
            self.assertEqual(1, len(session.advanced.what_changed()))

    def test_crud_operations_with_array_in_object_5_dynamic(self):
        with self.store.open_session() as session:
            family = Family(["Gracjan", "OG IT"])
            session.store(family, "family/gracjan")
            session.save_changes()

        with self.store.open_session() as session:
            new_family = session.load("family/gracjan")
            new_family.names = ["Gracjan"]
            self.assertEqual(1, len(session.advanced.what_changed()))

    def test_crud_operations_with_array_in_object_5_dict(self):
        with self.store.open_session() as session:
            family = {"names": ["Gracjan", "OG IT"]}
            session.store(family, "family/gracjan")
            session.save_changes()

        with self.store.open_session() as session:
            new_family = session.load("family/gracjan")
            new_family["names"] = ["Gracjan"]
            self.assertEqual(1, len(session.advanced.what_changed()))

    def test_crud_operations_with_array_of_arrays(self):
        with self.store.open_session() as session:
            a1 = Arr1(["a", "b"])
            a2 = Arr1(["c", "d"])
            arr = Arr2([a1, a2])
            session.store(arr, "arr/1")
            session.save_changes()

            new_arr = session.load("arr/1", Arr2)
            a1 = Arr1(["d", "c"])
            a2 = Arr1(["a", "b"])
            new_arr.arr1 = [a1, a2]

            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))
            change = changes["arr/1"]
            self.assertEqual(4, len(change))

            change = sorted(change, key=lambda ch: (ch.field_old_value, ch.field_new_value))

            self.assertEqual("a", change[0].field_old_value)
            self.assertEqual("d", change[0].field_new_value)

            self.assertEqual("b", change[1].field_old_value)
            self.assertEqual("c", change[1].field_new_value)

            self.assertEqual("c", change[2].field_old_value)
            self.assertEqual("a", change[2].field_new_value)

            self.assertEqual("d", change[3].field_old_value)
            self.assertEqual("b", change[3].field_new_value)

            session.save_changes()

        with self.store.open_session() as session:
            new_arr = session.load("arr/1", Arr2)
            a1 = Arr1(["q", "w"])
            a2 = Arr1(["a", "b"])
            new_arr.arr1 = [a1, a2]

            changes = session.advanced.what_changed()
            self.assertEqual(1, len(changes))

            change = changes["arr/1"]
            self.assertEqual(2, len(change))
            change = sorted(change, key=lambda ch: (ch.field_old_value, ch.field_new_value))

            self.assertEqual("c", change[0].field_old_value)
            self.assertEqual("w", change[0].field_new_value)

            self.assertEqual("d", change[1].field_old_value)
            self.assertEqual("q", change[1].field_new_value)

            session.save_changes()

    def test_crud_operations_with_null(self):
        with self.store.open_session() as session:
            user = User(None)
            session.store(user, "users/1")
            session.save_changes()

            user2 = session.load("users/1", User)
            self.assertEqual(0, len(session.advanced.what_changed()))
            user2.age = 3
            self.assertEqual(1, len(session.advanced.what_changed()))

    def test_can_update_property_to_null(self):
        with self.store.open_session() as session:
            user = User("user1")
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            user.name = None
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            self.assertIsNone(user.name)

    def test_crud_operations_with_what_changed(self):
        with self.store.open_session() as session:
            user1 = User("user1")
            session.store(user1, "users/1")

            user2 = User("user2", 1)
            session.store(user2, "users/2")

            user3 = User("user3", 1)
            session.store(user3, "users/3")

            user4 = User("user4")
            session.store(user4, "users/4")

            session.delete(user2)
            user3.age = 3

            self.assertEqual(4, len(session.advanced.what_changed()))
            session.save_changes()

            temp_user = session.load("users/2", User)
            self.assertIsNone(temp_user)

            temp_user = session.load("users/3", User)
            self.assertEqual(3, temp_user.age)

            user1 = session.load("users/1", User)
            user4 = session.load("users/4", User)

            session.delete(user4)
            user1.age = 10
            self.assertEqual(2, len(session.advanced.what_changed()))

            session.save_changes()
            temp_user = session.load("users/4", User)
            self.assertIsNone(temp_user)

            temp_user = session.load("users/1", User)
            self.assertEqual(10, temp_user.age)

    def test_crud_operations(self):
        with self.store.open_session() as session:
            user1 = User("user1")
            session.store(user1, "users/1")

            user2 = User("user2", 1)
            session.store(user2, "users/2")

            user3 = User("user3", 1)
            session.store(user3, "users/3")

            user4 = User("user4")
            session.store(user4, "users/4")

            session.delete(user2)
            user3.age = 3

            session.save_changes()

            temp_user = session.load("users/2", User)
            self.assertIsNone(temp_user)

            temp_user = session.load("users/3", User)
            self.assertEqual(3, temp_user.age)

            user1 = session.load("users/1", User)
            user4 = session.load("users/4", User)

            session.delete(user4)
            user1.age = 10
            session.save_changes()

            temp_user = session.load("users/4", User)
            self.assertIsNone(temp_user)

            temp_user = session.load("users/1", User)
            self.assertEqual(10, temp_user.age)
