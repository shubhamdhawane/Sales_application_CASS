-- create a keyspace for sales transaction generator "SalesApp" will be used with python cassandra-driver

CREATE KEYSPACE sales WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3 };
--CREATE KEYSPACE sales WITH replication = {'class': 'NetworkTopologyStrategy','
--ap-northeast-1':3,'us-east-1': 3 };
-- if authentication is enabled on cassandra
CREATE ROLE sales_user WITH SUPERUSER = false AND LOGIN = true AND PASSWORD = 'PassCode' ;
GRANT select ON KEYSPACE sales TO sales_user ;
GRANT modify ON KEYSPACE sales TO sales_user ;
LIST ALL PERMISSIONS OF sales_user ;

