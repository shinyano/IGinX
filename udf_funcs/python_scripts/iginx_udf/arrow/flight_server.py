import ast
import threading
import time

import pyarrow.flight
from pyarrow import flight
# from .flight_client_pool import FlightClientPool


class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates
        self.location = location

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        location = pyarrow.flight.Location.for_grpc_unix(self.location)
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = \
                    pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        print(key)
        self.flights[key] = reader.read_all()

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key].to_reader(64_000))

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            self.flights = {}
        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            # request
            threading.Thread(target=self._shutdown).start()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.shutdown()


client_pool = None


def start_server():
    threading.Thread(target=_start_server).start()


# def get_pool() -> FlightClientPool:
#     global client_pool
#     if client_pool is None:
#         client_pool = FlightClientPool("grpc+tcp://localhost:33333")
#     return client_pool


def _start_server():
    location = "grpc+tcp://localhost:33333"

    server = FlightServer("localhost", location, client_pool)
    print("Serving on", location)
    server.serve()


def shutdown_server():
    client = flight.FlightClient("grpc+tcp://localhost:33333")
    action = flight.Action("shutdown", b"")
    try:
        # 尝试执行 shutdown 动作
        result = client.do_action(action)
        for r in result:
            print("Response from server:", r.body.to_pybytes().decode())
    except Exception as e:
        print("Failed to shutdown server:", e)


if __name__ == '__main__':
    start_server()
