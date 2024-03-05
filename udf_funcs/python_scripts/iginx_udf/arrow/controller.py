import pyarrow as pa
import tempfile


class ArrowController:
    def __init__(self):
        pass

    def receive(self):
        import pyarrow as pa
        from pyarrow.cffi import ffi
        # array = pa.Array._import_from_c()
        c_stream = ffi.new("struct ArrowArrayStream*")

