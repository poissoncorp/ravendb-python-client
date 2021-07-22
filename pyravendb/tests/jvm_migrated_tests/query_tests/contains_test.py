from pyravendb.tests.test_base import TestBase, UserWithId


class UserWithFavs(UserWithId):
    def __init__(self, name=None, age=None, key=None, favourites=None):
        super(UserWithFavs, self).__init__(name, age, key)
        self.favourites = favourites


class TestRegexQuery(TestBase):
    def setUp(self):
        super(TestRegexQuery, self).setUp()

    def test_contains(self):
        with self.store.open_session() as session:
            session.store(UserWithFavs("John", None, None, ["java", "c#"]))
            session.store(UserWithFavs("Tarzan", None, None, ["java", "go"]))
            session.store(UserWithFavs("Jane", None, None, ["pascal"]))

            session.save_changes()

        with self.store.open_session() as session:
            pascal_or_go_dev_names = list(
                session.query(UserWithFavs).contains_any("favourites", ["pascal", "go"]).select("name")
            )
            self.assertEqual(2, len(pascal_or_go_dev_names))
            self.assertSequenceContainsElements(
                list(map(lambda user: user.name, pascal_or_go_dev_names)), "Jane", "Tarzan"
            )

        with self.store.open_session() as session:
            java_devs = list(session.query(UserWithFavs).contains_all("favourites", ["java"]).select("name"))
            self.assertEqual(2, len(java_devs))
            self.assertSequenceContainsElements(list(map(lambda user: user.name, java_devs)), "John", "Tarzan")
