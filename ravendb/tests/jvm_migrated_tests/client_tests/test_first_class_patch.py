from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


@dataclass
class Pet:
    name: str = None
    kind: str = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, str]) -> Pet:
        return cls(json_dict["name"], json_dict["kind"])


@dataclass
class Friend:
    name: str = None
    age: int = None
    pet: Pet = None

    @classmethod
    def from_json(cls, json_dict: Dict) -> Friend:
        return cls(
            json_dict["name"],
            int(json_dict["age"]) if json_dict["age"] is not None else None,
            Pet.from_json(json_dict["pet"]) if json_dict["pet"] is not None else None,
        )


@dataclass
class Stuff:
    key: int = None
    phone: str = None
    pet: Pet = None
    friend: Friend = None
    dic: Dict[str, str] = None

    @classmethod
    def from_json(cls, json_dict: Dict) -> Stuff:
        return cls(
            int(json_dict["key"]) if json_dict["key"] is not None else None,
            json_dict["phone"],
            Pet.from_json(json_dict["pet"]) if json_dict["pet"] is not None else None,
        )


@dataclass
class User:
    stuff: List[Stuff] = None
    last_login: datetime = None
    numbers: List[int] = None

    @classmethod
    def from_json(cls, json_dict: Dict) -> User:
        return cls(
            [Stuff.from_json(jdict) if jdict is not None else None for jdict in json_dict["stuff"]]
            if json_dict["stuff"] is not None
            else None,
            Utils.string_to_datetime(json_dict["last_login"]),
            [int(number) if number is not None else None for number in json_dict["numbers"]],
        )


class TestFirstClassPatch(TestBase):
    def setUp(self):
        super(TestFirstClassPatch, self).setUp()
        self.doc_id = "users/1-A"

    def test_can_patch(self):
        stuff = [Stuff(), None, None]
        stuff[0].key = 6

        user = User(numbers=[66], stuff=stuff)

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        now = datetime.now()

        with self.store.open_session() as session:
            session.advanced.patch(self.doc_id, "numbers[0]", 31)
            session.advanced.patch(self.doc_id, "last_login", now)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(31, loaded.numbers[0])
            self.assertEqual(now, loaded.last_login)

            session.advanced.patch(loaded, "stuff[0].phone", "123456")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual("123456", loaded.stuff[0].phone)