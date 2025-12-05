from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import time

# =============================
#  CONNECT TO CASSANDRA
# =============================

auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(["172.31.30.101", "172.31.28.68"], auth_provider=auth)
session = cluster.connect("sales")


# =============================
#  BAD QUERY TEST FUNCTIONS
# =============================

def test_full_table_scan():
    print("\n❌ TEST: FULL TABLE SCAN (Very dangerous)")
    query = "SELECT * FROM sales_orders LIMIT 50000;"
    run_bad_query(query)


def test_missing_partition_key():
    print("\n❌ TEST: MISSING PARTITION KEY (Coordinator scans all nodes)")
    query = """
        SELECT order_code, order_total
        FROM sales_orders
        WHERE user_platform='ANDROID' ALLOW FILTERING;
    """
    run_bad_query(query)


def test_allow_filtering_low_cardinality():
    print("\n❌ TEST: ALLOW FILTERING - Low Cardinality Column (Disaster)")
    query = """
        SELECT order_code
        FROM sales_orders
        WHERE order_status='DELIVERED' ALLOW FILTERING;
    """
    run_bad_query(query)


def test_wide_partition():
    print("\n❌ TEST: WIDE PARTITION (100k+ rows)")
    query = """
        SELECT order_code, order_total
        FROM sales_orders
        WHERE order_date='2024-12-03';
    """  # If order_date is partition key → wide partition
    run_bad_query(query)


def test_large_in_query():
    print("\n❌ TEST: Large IN clause (Coordinator overloading)")
    query = """
        SELECT order_code
        FROM sales_orders
        WHERE order_date IN ('2024-12-01','2024-12-02','2024-12-03','2024-12-04');
    """
    run_bad_query(query)


def test_secondary_index_bad():
    print("\n❌ TEST: Secondary Index on High Cardinality Column")
    query = """
        SELECT order_code
        FROM sales_orders
        WHERE user_email_id='someone@example.com';
    """
    run_bad_query(query)


def test_hot_partition():
    print("\n❌ TEST: HOT PARTITION (Same PK repeatedly)")
    for i in range(50000):
        query = SimpleStatement("""
            INSERT INTO sales_orders (order_date, order_date_hour, order_code, order_total)
            VALUES ('2024-12-03', 10, 'CODE_%s', 100);
        """ % i)
        session.execute(query)
    print("🔥 Inserted 50k rows into SAME partition (Hotspot)")


def test_wrong_clustering_scan():
    print("\n❌ TEST: Wrong clustering order scan")
    query = """
        SELECT order_code
        FROM sales_orders
        WHERE order_date='2024-12-03'
        AND order_date_hour=10
        AND order_total > 100;      -- NOT CLUSTERED => full scan
    """
    run_bad_query(query)


def test_big_fetch_size():
    print("\n❌ TEST: Huge fetch size (Memory explosion)")
    statement = SimpleStatement(
        "SELECT * FROM sales_orders;",
        fetch_size=50000   # default 5000 → here we force big pages
    )
    run_bad_query(statement)


# =============================
#   BAD QUERY EXECUTOR
# =============================

def run_bad_query(query):
    try:
        start = time.time()
        rows = session.execute(query)
        count = 0
        for r in rows:
            count += 1
            if count > 2000:
                break  # Stop massive output
        end = time.time()

        print(f"⚠ Rows Read: {count}")
        print(f"⏱ Query Time: {end - start:.2f} sec\n")

    except Exception as e:
        print(f"💥 ERROR: {e}\n")


# =============================
#   RUN ALL BAD QUERIES
# =============================

print("\n==============================")
print(" ⚠ CASSANDRA BAD QUERY TESTS ⚠")
print("==============================")

test_full_table_scan()
test_missing_partition_key()
test_allow_filtering_low_cardinality()
test_wide_partition()
test_large_in_query()
test_secondary_index_bad()
test_wrong_clustering_scan()
test_big_fetch_size()

# Optional: hotspot test
# test_hot_partition()

cluster.shutdown()
print("\n✔ Done. Connection closed.\n")
