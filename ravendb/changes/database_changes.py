import base64
import ssl
from threading import Lock
from typing import TYPE_CHECKING, Dict, Optional

from websocket import WebSocket

from ravendb.changes.observers import Observable
from ravendb.changes.types import (
    DocumentChange,
    IndexChange,
    TimeSeriesChange,
    CounterChange,
    OperationStatusChange,
    TopologyChange,
)
from ravendb.serverwide.commands import GetTcpInfoCommand
from ravendb.tools.parsers import IncrementalJsonParser
import websocket
from ravendb.exceptions.exceptions import NotSupportedException
from ravendb.exceptions.exceptions import ChangeProcessingException
from ravendb.tools.utils import Utils
import copy
from time import sleep
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future
import logging
import sys

if TYPE_CHECKING:
    from ravendb.http.request_executor import RequestExecutor


class DatabaseChanges:
    def __init__(self, request_executor: "RequestExecutor", database_name, on_close, on_error=None, executor=None):
        self._request_executor = request_executor
        self._conventions = request_executor.conventions
        self._database_name = database_name

        self._command_id = 0
        self.client_websocket = websocket.WebSocket()
        self._closed = False
        self._on_close = on_close
        self.on_error = on_error
        self._observables = dict()

        self._executor = executor if executor else ThreadPoolExecutor(max_workers=10)
        self._worker = self._executor.submit(self.do_work)
        self.send_lock = Lock()
        self._confirmations_lock = Lock()
        self._confirmations = {}

        self._command_id = 0
        self._immediate_connection = 0

        self._logger = logging.getLogger("database_changes")
        handler = logging.FileHandler("changes.log")
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

    def _get_server_certificate(self) -> Optional[str]:
        cmd = GetTcpInfoCommand(self._request_executor.url)
        self._request_executor.execute_command(cmd)
        return cmd.result.certificate

    def do_work(self):
        preferred_node = self._request_executor.preferred_node.current_node  # todo: refactor, protected access
        url = (
            f"{preferred_node.url}/databases/{self._database_name}/changes".replace("http://", "ws://")
            .lower()
            .replace("https://", "wss://")
            .replace(".fiddler", "")
        )

        while not self._closed:
            try:
                if not self.client_websocket.connected:
                    if self._request_executor.certificate_path:
                        # Get server certificate via HTTPS and prepare SSL context
                        server_certificate = base64.b64decode(self._get_server_certificate())
                        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
                        ssl_context.load_cert_chain(self._request_executor.certificate_path)

                        # Connect WebSocket providing SSL
                        self.client_websocket = WebSocket(sslopt={"context": ssl_context})
                        self.client_websocket.connect(url, suppress_origin=True)

                        # Server certificate authentication
                        server_certificate_from_tls = self.client_websocket.sock.getpeercert(True)
                        if server_certificate != server_certificate_from_tls:
                            raise ValueError("Certificates don't match")

                        pass
                    else:
                        self.client_websocket.connect(url)

                    for observables in self._observables.values():
                        for observer in observables.values():
                            observer.set(self._executor.submit(observer.on_connect))
                    self._immediate_connection = 1
                self.process_changes()
            except ChangeProcessingException as e:
                self.notify_about_error(e)
                continue
            except websocket.WebSocketException as e:
                if getattr(sys, "gettrace", None):
                    self._logger.error(e)
                self.notify_about_error(e)
                return
            except Exception as e:
                self._immediate_connection = 0
                self.client_websocket = websocket.WebSocket()
                self.notify_about_error(e)
            sleep(1)

    def process_changes(self):
        parser = IncrementalJsonParser(self.client_websocket, is_websocket=True)
        while not self._closed:
            try:
                response = parser.next_object()
                if response:
                    response_type = response.get("Type", None)
                    if not response_type:
                        continue
                    if response_type == "Error":
                        exception = response["Exception"]
                        self.notify_about_error(Exception(exception))
                    elif response_type == "Confirm":
                        command_id = response.get("CommandId", None)
                        if command_id and command_id in self._confirmations:
                            with self._confirmations_lock:
                                future = self._confirmations.pop(command_id)
                                future.set_result("done complete future")
                    else:
                        value = response.get("Value", None)
                        self._notify_subscribers(response_type, value, copy.copy(self._observables[response_type]))
            except Exception as e:
                self.notify_about_error(e)
                raise ChangeProcessingException(e)

    def _notify_subscribers(self, type_of_change: str, value: Dict, observables: Dict[str, Observable]):
        if type_of_change == "DocumentChange":
            result = DocumentChange.from_json(value)
        elif type_of_change == "IndexChange":
            result = IndexChange.from_json(value)
        elif type_of_change == "TimeSeriesChange":
            result = TimeSeriesChange.from_json(value)
        elif type_of_change == "CounterChange":
            result = CounterChange.from_json(value)
        elif type_of_change == "OperationStatusChange":
            result = OperationStatusChange.from_json(value)
        elif type_of_change == "TopologyChange":
            result = TopologyChange.from_json(value)
        else:
            raise NotSupportedException(type_of_change)

        for observable in observables.values():
            observable.send(result)

    def close(self):
        self._closed = True
        self.client_websocket.close()

        for observable in self._observables.values():
            for observer in observable.values():
                observer.close()

        with self._confirmations_lock:
            for confirmation in self._confirmations.values():
                confirmation.cancel()

        self._observables.clear()
        if self._on_close:
            self._on_close(self._database_name)

        self._executor.shutdown(wait=True)

    def notify_about_error(self, e):
        if self.on_error:
            self.on_error(e)

        for _, observables in self._observables.items():
            for observer in observables.values():
                observer.error(e)

    def for_all_documents(self) -> Observable[DocumentChange]:
        # todo: ResourceWarning: unclosed socket
        observable = self.get_or_add_observable("DocumentChange", "all-docs", "watch-docs", "unwatch-docs", None)(
            lambda x: True
        )
        return observable

    def for_all_operations(self) -> Observable[OperationStatusChange]:
        observable = self.get_or_add_observable(
            "OperationsStatusChange",
            "all-operations",
            "watch-operations",
            "unwatch-operations",
            None,
        )(lambda x: True)
        return observable

    def for_all_indexes(self) -> Observable[IndexChange]:
        observable = self.get_or_add_observable("IndexChange", "all-indexes", "watch-indexes", "unwatch-indexes", None)(
            lambda x: True
        )
        return observable

    def for_index(self, index_name) -> Observable[IndexChange]:
        observable = self.get_or_add_observable(
            "IndexChange",
            "indexes/" + index_name,
            "watch-index",
            "unwatch-index",
            index_name,
        )(lambda x: x.name.casefold() == index_name.casefold())
        return observable

    def for_operation_id(self, operation_id) -> Observable[OperationStatusChange]:
        observable = self.get_or_add_observable(
            "OperationsStatusChange",
            "operations/" + str(operation_id),
            "watch-operation",
            "unwatch-operation",
            str(operation_id),
        )(lambda x: x.operation_id == str(operation_id))
        return observable

    def for_document(self, doc_id) -> Observable[DocumentChange]:
        observable = self.get_or_add_observable("DocumentChange", "docs/" + doc_id, "watch-doc", "unwatch-doc", doc_id)(
            lambda x: x.key.casefold() == doc_id.casefold()
        )
        return observable

    def for_documents_start_with(self, doc_id_prefix) -> Observable[DocumentChange]:
        observable = self.get_or_add_observable(
            "DocumentChange",
            "prefixes/" + doc_id_prefix,
            "watch-prefix",
            "unwatch-prefix",
            doc_id_prefix,
        )(lambda x: x.key is not None and x.key.casefold().startswith(doc_id_prefix.casefold()))
        return observable

    def for_documents_in_collection(self, collection_name) -> Observable[DocumentChange]:
        observable = self.get_or_add_observable(
            "DocumentChange",
            "collections/" + collection_name,
            "watch-collection",
            "unwatch-collection",
            collection_name,
        )(lambda x: x.collection_name.casefold() == collection_name.casefold())
        return observable

    def for_all_time_series(self) -> Observable[TimeSeriesChange]:
        observable = self.get_or_add_observable(
            "TimeSeriesChange",
            "all-timeseries",
            "watch-all-timeseries",
            "unwatch-all-timeseries",
            None,
        )(lambda x: True)
        return observable

    def for_time_series(self, time_series_name) -> Observable[TimeSeriesChange]:
        if not time_series_name:
            raise ValueError("time_series_name cannot be None or empty")
        observable = self.get_or_add_observable(
            "TimeSeriesChange",
            f"timeseries/{time_series_name}",
            "watch-timeseries",
            "unwatch-timeseries",
            time_series_name,
        )(lambda x: x.name.casefold() == time_series_name.casefold())
        return observable

    def for_time_series_of_document(self, doc_id, time_series_name=None) -> Observable[TimeSeriesChange]:
        """
        Can subscribe to all time series changes that associated with the document or
        by passing the time series name only for a specific time series
        """
        if not doc_id:
            raise ValueError("doc_id cannot be None or empty")

        def get_lambda():
            if time_series_name:
                return lambda x: x.document_id.casefold() == doc_id.casefold() and x.name.casefold()
            return lambda x: x.document_id.casefold() == doc_id.casefold()

        name = f"document/{doc_id}/timeseries{f'/{time_series_name}' if time_series_name else ''}"
        watch_command = "watch-document-timeseries" if time_series_name else "watch-all-document-timeseries"
        unwatch_command = "unwatch-document-timeseries" if time_series_name else "unwatch-all-document-timeseries"
        value = doc_id if time_series_name is None else None
        values = [doc_id, time_series_name] if time_series_name is not None else None
        observable = self.get_or_add_observable(
            "TimeSeriesChange",
            name,
            watch_command,
            unwatch_command,
            value=value,
            values=values,
        )(get_lambda())
        return observable

    def for_all_counters(self) -> Observable[CounterChange]:
        observable = self.get_or_add_observable(
            "CounterChange", "all-counters", "watch-counters", "unwatch-counters", None
        )(lambda x: True)
        return observable

    def for_counter(self, counter_name) -> Observable[CounterChange]:
        if not counter_name:
            raise ValueError("counter_name cannot be None or empty")
        observable = self.get_or_add_observable(
            "CounterChange",
            f"counter/{counter_name}",
            "watch-counter",
            "unwatch-counter",
            counter_name,
        )(lambda x: x.name.casefold() == counter_name.casefold())
        return observable

    def for_counters_of_document(self, doc_id) -> Observable[CounterChange]:
        """
        Can subscribe to all counters changes that associated with the document or
        """
        if not doc_id:
            raise ValueError("doc_id cannot be None or empty")

        observable = self.get_or_add_observable(
            "CounterChange",
            f"document/{doc_id}/counter",
            "watch-document-counters",
            "unwatch-document-counters",
            value=doc_id,
            values=None,
        )(lambda x: x.document_id.casefold() == doc_id.casefold())
        return observable

    def for_counter_of_document(self, doc_id, counter_name) -> Observable[CounterChange]:
        """
        Can subscribe to all counters changes that associated with the document and for counter name
        """
        if not doc_id:
            raise ValueError("doc_id cannot be None or empty")
        if not counter_name:
            raise ValueError("counter_name cannot be None or empty")

        observable = self.get_or_add_observable(
            "CounterChange",
            f"document/{doc_id}/counter/{counter_name}",
            "watch-document-counter",
            "unwatch-document-counter",
            value=None,
            values=[doc_id, counter_name],
        )(lambda x: x.document_id.casefold() == doc_id.casefold() and x.name.casefold())
        return observable

    def get_or_add_observable(self, group, name, watch_command, unwatch_command, value, values=None):
        if group not in self._observables:
            self._observables[group] = {}

        if name not in self._observables[group]:

            def on_disconnect():
                try:
                    if self.client_websocket.connected:
                        self.send(unwatch_command, value, values)
                except websocket.WebSocketException:
                    pass

            def on_connect():
                self.send(watch_command, value, values)

            observable = Observable(
                on_connect=on_connect,
                on_disconnect=on_disconnect,
                executor=self._executor,
            )
            self._observables[group][name] = observable
            if self._immediate_connection != 0:
                observable.set(self._executor.submit(observable.on_connect))
        return self._observables[group][name]

    def send(self, command, value, values=None):
        current_command_id = 0
        future = Future()
        try:
            with self.send_lock:
                self._command_id += 1
                current_command_id += self._command_id
                data_dict = {
                    "CommandId": current_command_id,
                    "Command": command,
                    "Param": value,
                }
                if values:
                    data_dict["Params"] = values
                data = Utils.dict_to_bytes(data_dict)
                with self._confirmations_lock:
                    self._confirmations[current_command_id] = future
                self.client_websocket.send(data)

                try:
                    future.result(timeout=15)
                except TimeoutError:
                    future.cancel()
                    raise TimeoutError("Did not get a confirmation for command #" + str(current_command_id))
                except Exception as e:
                    if getattr(sys, "gettrace", None):
                        self._logger.info("The coroutine raised an exception: {!r}".format(e))
        except websocket.WebSocketConnectionClosedException:
            pass
