#!/bin/bash
set -e

# =======================
# Cassandra configuration
# =======================
CASS_USER="cassandra"
CASS_PASS="cassandra"

CASS_NODE1="172.31.30.220"
CASS_NODE2="172.31.26.29"

CASS_CONTACT_POINTS="[\"${CASS_NODE1}\",\"${CASS_NODE2}\"]"

APP_HOME="/home/ec2-user/Sales_application_CASS"

echo "===== Updating system ====="
sudo yum update -y

echo "===== Installing required packages ====="
sudo yum install -y python-pip git

echo "===== Installing Cassandra Python driver ====="
sudo pip install cassandra-driver

echo "===== Cloning application repository ====="
cd /home/ec2-user
git clone https://github.com/shubhamdhawane/Sales_application_CASS.git

cd ${APP_HOME}

echo "===== Configuring global settings ====="
mv globalSettings.py_large globalSettings.py

echo "===== Updating Cassandra contact points ====="
sed -i "s|^CASS_CONTACT_POINTS.*|CASS_CONTACT_POINTS = ${CASS_CONTACT_POINTS};|" cassConnectionManager.py

echo "===== Setting PEM file permissions ====="
chmod 400 n.verg-vila.pem

echo "===== Updating keyspace strategy ====="
sed -i 's/^--\(CREATE KEYSPACE sales WITH replication =.*SimpleStrategy.*\)/\1/' 01_cassandra_sales_keyspace.cql
sed -i 's/^\(CREATE KEYSPACE sales WITH replication =.*NetworkTopologyStrategy.*\)/--\1/' 01_cassandra_sales_keyspace.cql

echo "===== Creating keyspace and tables ====="
cqlsh ${CASS_NODE1} -u ${CASS_USER} -p ${CASS_PASS} -f 01_cassandra_sales_keyspace.cql
cqlsh ${CASS_NODE1} -u ${CASS_USER} -p ${CASS_PASS} -f 02_sales_create_tables.cql
cqlsh ${CASS_NODE1} -u ${CASS_USER} -p ${CASS_PASS} -f 03_load_data_in_lookup_tables.cql

echo "===== Running initial Python scripts ====="
python cassConnectionManager.py
python SalesApp_GenerateUsers.py
python SalesApp_GenerateProducts.py

echo "===== Updating read_req Cassandra contact points ====="
sed -i "s|^cluster = Cluster(\[.*\],|cluster = Cluster(${CASS_CONTACT_POINTS},|" read_req.py

echo "===== Configuring cron jobs ====="
(crontab -l 2>/dev/null; \
echo "* * * * * /usr/bin/python ${APP_HOME}/SalesApp_GenerateOrders.py >> /home/ec2-user/write.log 2>&1"; \
echo "* * * * * /usr/bin/python ${APP_HOME}/read_req.py >> /home/ec2-user/read.log 2>&1") | crontab -

echo "===== Setup completed successfully ====="
