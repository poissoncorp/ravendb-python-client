from __future__ import annotations
import json
from typing import Optional, Generic, TypeVar, Dict, TYPE_CHECKING, Type, Collection

import requests

from ravendb.primitives import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.compare_exchange.compare_exchange import (
    CompareExchangeValue,
    CompareExchangeSessionValue,
)
from ravendb.documents.operations.compare_exchange.compare_exchange_value_result_parser import (
    CompareExchangeValueResultParser,
)
from ravendb.documents.operations.definitions import IOperation
from ravendb.http.http_cache import HttpCache
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.documents.session.entity_to_json import EntityToJsonStatic
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator

_T = TypeVar("_T")

if TYPE_CHECKING:
    from ravendb.documents.store import DocumentStore


class CompareExchangeResult(Generic[_T]):
    def __init__(self, value: _T, index: int, successful: bool):
        self.value = value
        self.index = index
        self.successful = successful

    @classmethod
    def parse_from_string(
        cls, object_type: type, response_string: str, conventions: DocumentConventions
    ) -> CompareExchangeResult:
        response: dict = json.loads(response_string)

        index = response.get("Index", None)
        if index is None:
            raise RuntimeError("Response is invalid. Index is missing.")

        successful = response.get("Successful")
        raw = response.get("Value")

        val = None

        if raw:
            val = raw.get(constants.CompareExchange.OBJECT_FIELD_NAME)

        if val is None:
            return cls(Utils.json_default(object_type), index, successful)

        result = (
            Utils.convert_json_dict_to_object(val, object_type) if not isinstance(val, (str, int, float, bool)) else val
        )

        return cls(result, index, successful)


class PutCompareExchangeValueOperation(IOperation[CompareExchangeResult], Generic[_T]):
    def __init__(self, key: str, value: _T, index: int, metadata: MetadataAsDictionary = None):
        self._key = key
        self._value = value
        self._index = index
        self._metadata = metadata

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.__PutCompareExchangeValueCommand(self._key, self._value, self._index, self._metadata, conventions)

    class __PutCompareExchangeValueCommand(RavenCommand[_T], RaftCommand, Generic[_T]):
        def __init__(
            self, key: str, value: _T, index: int, metadata: MetadataAsDictionary, conventions: DocumentConventions
        ):
            if not key:
                raise ValueError("The key argument must have value")

            if index < 0:
                raise ValueError("Index must be a non-negative number")

            super().__init__(CompareExchangeResult[_T])

            self._key = key
            self._value = value
            self._index = index
            self._metadata = metadata
            self._conventions = conventions or DocumentConventions()

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/cmpxchg?key={Utils.quote_key(self._key)}&index={self._index}"
            object_and_value = {constants.CompareExchange.OBJECT_FIELD_NAME: self._value}
            json_dict = EntityToJsonStatic.convert_entity_to_json(object_and_value, self._conventions, None, False)
            if self._metadata:
                metadata = CompareExchangeSessionValue.prepare_metadata_for_put(
                    self._key, self._metadata, self._conventions
                )
                json_dict[constants.Documents.Metadata.KEY] = metadata

            return requests.Request("PUT", url, data=json_dict)

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeResult.parse_from_string(type(self._value), response, self._conventions)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetCompareExchangeValueOperation(IOperation[CompareExchangeValue[_T]], Generic[_T]):
    def __init__(self, key: str, object_type: Type[_T], materialize_metadata: bool = True):
        self._key = key
        self._object_type = object_type
        self._materialize_metadata = materialize_metadata

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.GetCompareExchangeValueCommand(
            self._key, self._object_type, self._materialize_metadata, conventions
        )

    class GetCompareExchangeValueCommand(RavenCommand[CompareExchangeValue[_T]]):
        def __init__(
            self, key: str, object_type: Type[_T], materialize_metadata: bool, conventions: DocumentConventions
        ):
            if not key:
                raise ValueError("The key argument must have value")
            super().__init__(CompareExchangeValue[_T])
            self._key = key
            self._object_type = object_type
            self._materialize_metadata = materialize_metadata
            self._conventions = conventions

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/cmpxchg?key={Utils.quote_key(self._key)}"
            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeValueResultParser.get_value(
                self._object_type, response, self._materialize_metadata, self._conventions
            )


class DeleteCompareExchangeValueOperation(IOperation[CompareExchangeResult[_T]], Generic[_T]):
    def __init__(self, object_type: type, key: str, index: int):
        self._key = key
        self._object_type = object_type
        self._index = index

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.RemoveCompareExchangeCommand[_T](self._object_type, self._key, self._index, conventions)

    class RemoveCompareExchangeCommand(RavenCommand[CompareExchangeResult[_T]], RaftCommand, Generic[_T]):
        def __init__(self, object_type: type, key: str, index: int, conventions: DocumentConventions):
            if not key:
                raise ValueError("The key must have value")

            super().__init__(CompareExchangeResult[_T])
            self._object_type = object_type
            self._key = key
            self._index = index
            self._conventions = conventions

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "DELETE",
                f"{node.url}/databases/{node.database}/cmpxchg?key={Utils.quote_key(self._key)}&index={self._index}",
            )

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeResult.parse_from_string(self._object_type, response, self._conventions)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetCompareExchangeValuesOperation(IOperation[Dict[str, CompareExchangeValue[_T]]], Generic[_T]):
    def __init__(
        self,
        keys: Optional[Collection[str]] = None,
        object_type: Optional[Type[_T]] = None,
        materialize_metadata: Optional[bool] = True,
    ):
        self._materialize_metadata = materialize_metadata
        self._keys = keys
        self._object_type = object_type
        self._start = None
        self._page_size = None
        self._start_with = None

    @classmethod
    def create_for_start_with(
        cls,
        start_with: str,
        start: Optional[int] = None,
        page_size: Optional[int] = None,
        object_type: Optional[Type[_T]] = None,
    ) -> GetCompareExchangeValuesOperation:
        operation = cls(None, object_type, True)
        operation._starts_with = start_with
        operation._start = start
        operation._page_size = page_size
        return operation

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.GetCompareExchangeValuesCommand(self, self._materialize_metadata, conventions)

    class GetCompareExchangeValuesCommand(RavenCommand[Dict[str, CompareExchangeValue[_T]]], Generic[_T]):
        def __init__(
            self,
            operation: GetCompareExchangeValuesOperation[_T],
            materialize_metadata: bool,
            conventions: DocumentConventions,
        ):
            super().__init__(dict)
            self._operation = operation
            self._materialize_metadata = materialize_metadata
            self._conventions = conventions

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            path_builder = [node.url, "/databases/", node.database, "/cmpxchg?"]

            if self._operation._keys:
                for key in self._operation._keys:
                    path_builder.append("&key=")
                    path_builder.append(Utils.quote_key(key))
            else:
                if self._operation._start_with:
                    path_builder.append("&startsWith=")
                    path_builder.append(Utils.quote_key(self._operation._start_with))
                if self._operation._start:
                    path_builder.append("&start=")
                    path_builder.append(Utils.quote_key(self._operation._start))
                if self._operation._page_size:
                    path_builder.append("&pageSize=")
                    path_builder.append(Utils.quote_key(self._operation._page_size))

            return requests.Request("GET", "".join(path_builder))

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeValueResultParser.get_values(
                self._operation._object_type, response, self._materialize_metadata, self._conventions
            )
