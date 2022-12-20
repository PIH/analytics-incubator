# openmrs-mysql

In order for the Debezium-based CDC to work in any of these examples, we need a source database that has the mysql
binlog enabled.  This requires using either a MySQL 5.6 or 5.7 instance with row-level bin logging explicitly enabled 
in the MySQL configuration, or it requires MySQL 8.x where row-level bin logging has been enabled by default.

The Debezium Flink MySQL connector requires MySQL 5.7 or higher.

To upgrade from 5.6 -> 5.7 -> 8, the following steps can be followed:

1. Get a MySQL 5.6 instance

```shell
docker-compose -f docker-compose-mysql5.6.yml up -d
```

2. Import the starting DB

```shell
./setup.sh
```

3. Stop and restart with MySQL 5.7

```shell
docker-compose down
docker-compose -f docker-compose-mysql5.7.yml up -d
```

4. Execute the upgrade script

```shell
./upgrade.sh
```

One could stop here if MySQL 5.7 is the target.  To got to MySQL 8 from here, just stop and restart with MySQL 8

```shell
docker-compose down
docker-compose -f docker-compose-mysql8.yml up -d
```

Going forward, one can rename whatever docker-compose yml file that represents the one in use and rename it to docker-compose.yml.

Then:

```shell
docker-compose down
docker-compose up -d 
```