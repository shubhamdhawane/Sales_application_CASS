from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Cassandra Connection Configuration
CASSANDRA_HOSTS = ['172.31.35.1', '172.31.35.173', '172.31.38.78']  # Correctly formatted as a list
USERNAME = 'cassandra'
PASSWORD = 'cassandra'
KEYSPACE = 'sales'

# Connect to Cassandra
def connect_to_cassandra():
    auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
    cluster = Cluster(CASSANDRA_HOSTS, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    return session

# Generate and Execute Read Queries
def generate_and_execute_queries(session, table_name, column_name, values):
    results = []
    for value in values:
        query = f"SELECT * FROM {table_name} WHERE {column_name} = %s ALLOW FILTERING"
        try:
            rows = session.execute(query, [value])
            for row in rows:
                results.append(row)
        except Exception as e:
            print(f"Failed to execute query: {query} with value: {value}")
            print(f"Error: {e}")
    return results


# Main function
if __name__ == '__main__':
    table_name = 'products'
    column_name = 'product_category'
    values = ['Software', 'Collectibles', 'Electronics']

    try:
        session = connect_to_cassandra()
        print("Connected to Cassandra!")

        print(f"Executing queries on table: {table_name}")
        query_results = generate_and_execute_queries(session, table_name, column_name, values)

        print("Query Results:")
        for result in query_results:
            print(result)

    except Exception as ex:
        print(f"Error connecting to Cassandra: {ex}")

    finally:
        session.cluster.shutdown()
        print("Cassandra connection closed.")
