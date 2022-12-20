#!/bin/bash

pv prepare-db.sql | docker exec -i mysql sh -c 'exec mysql -u root -proot mysql'
pv humci.sql | docker exec -i mysql sh -c 'exec mysql -u root -proot humci'
pv clean-db.sql | docker exec -i mysql sh -c 'exec mysql -u root -proot humci'

