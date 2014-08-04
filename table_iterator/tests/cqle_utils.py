import cqlengine


_cqlengine_setup = False

TEST_CASSANDRA_HOST = 'localhost'
TEST_CASSANDRA_CONSISTENCY = 'ONE'


def setup_cqlengine():
    global _cqlengine_setup
    if not _cqlengine_setup:

        cqlengine.connection.setup(
            hosts=[TEST_CASSANDRA_HOST],
            consistency=1
        )

        _cqlengine_setup = True