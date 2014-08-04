=======================
cqlengine TableIterator
=======================

The cqlengine TableIterator is an iterator that will page through an entire Cassandra table yielding the results one row at a time.

TableIterator provides a feature not available in cqlengine by default. It works across tables that have both composite
partition keys and composite clustering keys.

Usage
=====

from table_iterator import TableIterator


for row in TableIterator(MyCqlModel):
    print row
