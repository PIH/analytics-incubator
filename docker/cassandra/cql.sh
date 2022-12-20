#!/bin/bash

docker run -it --network analytics --name cqlsh --rm cassandra cqlsh cassandra

