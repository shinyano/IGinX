import pyarrow.flight as flight
import queue
import threading

class FlightClientPool:
    def __init__(self, server_location, pool_size=10, init_size=5):
        self.server_location = server_location
        self.pool_size = pool_size
        self.clients = queue.Queue(maxsize=pool_size)
        self.lock = threading.Lock()
        for i in range(init_size):
            client = self._create_client()
            self.return_client(client)

    def _create_client(self):
        # 创建新的 Flight 客户端连接
        return flight.connect(self.server_location)

    def get_client(self):
        with self.lock:
            if self.clients.empty():
                # 如果队列为空且未达到池大小限制，创建新的客户端
                return self._create_client()
            else:
                # 否则从队列中获取一个客户端实例
                return self.clients.get()

    def return_client(self, client):
        # 将客户端实例归还到队列中
        if self.clients.qsize() < self.pool_size:
            self.clients.put(client)
        else:
            # 如果池已满，则关闭多余的客户端连接
            client.close()

    def execute(self, func, *args, **kwargs):
        """
        从池中获取一个客户端实例，执行给定函数，然后归还客户端。
        func: 要执行的函数。此函数应接受 FlightClient 作为第一个参数。
        """
        client = self._get_client()
        try:
            result = func(client, *args, **kwargs)
        finally:
            self._return_client(client)
        return result
