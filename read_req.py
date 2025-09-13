from cassConnectionManager import cassConnect
from cassandra             import ConsistencyLevel
from cassandra.query       import SimpleStatement
from datetime              import datetime, date, timedelta
from globalSettings        import *
import sys
import random
import uuid
import math
import string
#from cassandra.cluster import Cluster
#from cassandra.auth import PlainTextAuthProvider
#from datetime import datetime

# --- Cassandra connection setup with authentication ---
#auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
#cluster = Cluster(["13.221.91.243", "18.183.188.248","13.221.91.243", "98.81.78.89"], auth_provider=auth_provider)
 # Replace with your actual IPs if needed
#session = cluster.connect('sales')

# --- Function to fetch hourly summary ---
def get_hourly_summary(order_date, order_hour):
    query = """
        SELECT order_code, order_grand_total, user_platform, user_state_code
        FROM sales_orders_hourly_summary
        WHERE order_date = %s AND order_date_hour = %s;
    """
    rows = session.execute(query, (order_date, order_hour))
    for row in rows:
        print(f"[HOURLY] Order: {row.order_code}, Total: {row.order_grand_total}, Platform: {row.user_platform}, State: {row.user_state_code}")

# --- Function to fetch daily summary ---
def get_daily_summary(order_date):
    query = """
        SELECT order_code, order_grand_total, user_platform, user_state_code
        FROM sales_orders_daily_summary
        WHERE order_date = %s;
    """
    rows = session.execute(query, (order_date,))
    for row in rows:
        print(f"[DAILY] Order: {row.order_code}, Total: {row.order_grand_total}, Platform: {row.user_platform}, State: {row.user_state_code}")

# --- Function to fetch complete order details ---
def get_order_details(order_date, order_hour):
    query = """
        SELECT order_timestamp, order_code, user_id, user_email_id, order_total
        FROM sales_orders
        WHERE order_date = %s AND order_date_hour = %s;
    """
    rows = session.execute(query, (order_date, order_hour))
    for row in rows:
        print(f"[ORDER] Code: {row.order_code}, User: {row.user_email_id}, Total: {row.order_total}")

# --- Auto read logic for current date and previous hour ---
now = datetime.utcnow()
today = now.date()
last_hour = now.hour - 1 if now.hour > 0 else 0

print(f"\n--- AUTO READ: Date = {today}, Hour = {last_hour} ---\n")
get_daily_summary(today)
get_hourly_summary(today, last_hour)
get_order_details(today, last_hour)

# --- Shutdown cluster connection ---
cluster.shutdown()
