from utils import ConnectionPool

connection_pool = ConnectionPool()

print(connection_pool.get_lowest_client_id())