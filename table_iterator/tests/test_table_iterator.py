import cqlengine
from cqlengine import BatchQuery
from cqlengine import columns
from unittest import TestCase

from table_iterator.tests.cqle_utils import setup_cqlengine
from table_iterator import TableIterator


class SimpleModel(cqlengine.Model):
    """
    Define a simple model with one partition key and one clustering key.
    """
    __keyspace__ = "tests"

    p_key = columns.Integer(partition_key=True)
    c_key = columns.Integer(primary_key=True)
    indexed_value = columns.Text(index=True)
    value = columns.Text()


class MultiPartitionKeyMultiClusteringKey(cqlengine.Model):
    """
    Define a model with a composite partition key and a composite clustering key.
    """
    __keyspace__ = "tests"

    p_key_a = columns.Integer(partition_key=True)
    p_key_b = columns.Integer(partition_key=True)

    c_key_a = columns.Integer(primary_key=True)
    c_key_b = columns.Integer(primary_key=True, clustering_order='DESC')
    c_key_c = columns.Integer(primary_key=True)

    value = columns.Text()


class TableIteratorFunctionalTests(TestCase):
    """
    Full functional tests for the TableIterator class.

    These tests are pretty database heavy so they take a while to run.
    """

    @classmethod
    def create_simple_model_objects(cls):
        """
        Create a bunch of instances of SimpleModel that we can test against.
        """

        # Setup SimpleModel related data.
        partition_keys = 101
        clustering_keys = 3
        expect_objects_count = partition_keys * clustering_keys

        cls.object_count_lookup[SimpleModel] = expect_objects_count
        cls.simple_model_unique_values = []
        cls.index_counter = {
            "EVEN": 0,
            "ODD": 0
        }

        with BatchQuery() as b:
            for p_key in range(partition_keys):
                for c_key in range(clustering_keys):
                    indexed_value = "EVEN" if c_key % 2 == 0 else "ODD"
                    cls.index_counter[indexed_value] += 1

                    unique_value = "{}:{}".format(p_key, c_key)

                    cls.simple_model_unique_values.append(unique_value)

                    SimpleModel.batch(b).create(
                        p_key=p_key,
                        c_key=c_key,
                        indexed_value=indexed_value,
                        value=unique_value)

        # Ensure that the unique values we generated are in fact unique.
        deduped_unique_values = list(set(cls.simple_model_unique_values))
        assert len(deduped_unique_values) == len(cls.simple_model_unique_values)

    @classmethod
    def create_multi_partition_key_multi_clustering_key_objects(cls):
        """
        Create a bunch of instances of MultiPartitionKeyMutliClustingKey objects.
        """
        a_partition_keys = 4
        b_partition_keys = 3
        a_clustering_keys = 5
        b_clustering_keys = 2
        c_clustering_keys = 3
        expect_objects_count = (
            a_partition_keys * b_partition_keys * a_clustering_keys * b_clustering_keys * c_clustering_keys
        )

        cls.multi_clust_unique_values = []
        cls.object_count_lookup[MultiPartitionKeyMultiClusteringKey] = expect_objects_count

        with BatchQuery() as b:
            for p_key_a in range(a_partition_keys):
                for p_key_b in range(b_partition_keys):
                    for c_key_a in range(a_clustering_keys):
                        for c_key_b in range(b_clustering_keys):
                            for c_key_c in range(c_clustering_keys):
                                unique_value = "{}:{}:{}:{}:{}".format(p_key_a, p_key_b, c_key_a, c_key_b, c_key_c)
                                cls.multi_clust_unique_values.append(unique_value)

                                MultiPartitionKeyMultiClusteringKey.batch(b).create(
                                    p_key_a=p_key_a,
                                    p_key_b=p_key_b,
                                    c_key_a=c_key_a,
                                    c_key_b=c_key_b,
                                    c_key_c=c_key_c,
                                    value=unique_value)

            # Test that the unique values are in fact unique.
            deduped_unique_values = list(set(cls.multi_clust_unique_values))
            assert len(deduped_unique_values) == len(cls.multi_clust_unique_values)

    @classmethod
    def setup_fixtures(cls):
        """
        Setup the test data fixtures.
        """
        # We track how many objects we create in each table so we can assert that our iterator works.
        cls.object_count_lookup = {}

        cls.create_simple_model_objects()
        cls.create_multi_partition_key_multi_clustering_key_objects()

    @classmethod
    def setUpClass(cls):
        super(TableIteratorFunctionalTests, cls).setUpClass()
        setup_cqlengine()
        cls.setup_fixtures()

    def test_simple_model_iteration(self):
        """
        Test that given a table populated with instances of the SimpleModel object,
        when you call TableIterator on that table using the default blocksize
        we expect to iterate over every object in that table.
        """
        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(SimpleModel):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.object_count_lookup[SimpleModel])
        self.assertEqual(len(unique_object_collector), self.object_count_lookup[SimpleModel])

    def test_simple_model_iteration_with_index_filter(self):
        """
        Test that given a table populated with instances of the SimpleModel object
        when you call a TableIterator on that table using the default blocksize bit
        specifying an indexed column value to filter with
        we expect to iterate over every object in that table which contains the indexed value.
        """
        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(SimpleModel, indexed_value='EVEN'):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.index_counter['EVEN'])
        self.assertEqual(len(unique_object_collector), self.index_counter['EVEN'])

        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(SimpleModel, indexed_value='ODD'):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.index_counter['ODD'])
        self.assertEqual(len(unique_object_collector), self.index_counter['ODD'])

    def test_composite_clustering_key_model(self):
        """
        Test that when we are given a table populated with MultiPartitionKeyMultiClusteringKey objects
        when we call the TableIterator on that table using the default blocksize
        we expect to iterate over every row in that table.
        """
        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(MultiPartitionKeyMultiClusteringKey):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])
        self.assertEqual(len(unique_object_collector), self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])

    def test_composite_clustering_key_model_blocksize_10(self):
        """
        Test that if we are given a table populated with MultiPartitionKeyMultiClusteringKey objects
        when we call the TableIterator on that table and specify a small blocksize (10)
        we iterate over every object in that table.
        """
        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(MultiPartitionKeyMultiClusteringKey, blocksize=10):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])
        self.assertEqual(len(unique_object_collector), self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])

    def test_composite_clustering_key_model_odd_blocksize(self):
        """
        Test that if we are given a table populated with MultiPartitionKeyMultiClusteringKey objects
        when we call the TableIterator on that table and specify a moderately sized (less than the total table size)
        and odd numbered blocksize
        we iterate over every object in that table.
        """
        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(MultiPartitionKeyMultiClusteringKey, blocksize=11):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])
        self.assertEqual(len(unique_object_collector), self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])

    def test_composite_clustering_key_model_blocksize_all(self):
        """
        Test that if we are given a table populated with MultiPartitionKeyMultiClusteringKey objects
        when we call the TableIterator on that table and specify a blocksize larger than the count of objects in the
        table (100000)
        we iterate over every object in that table.
        """
        total_object_counter = 0
        unique_object_collector = set()

        for row in TableIterator(MultiPartitionKeyMultiClusteringKey, blocksize=100000):
            total_object_counter += 1
            unique_object_collector.add(row.value)

        self.assertEqual(total_object_counter, self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])
        self.assertEqual(len(unique_object_collector), self.object_count_lookup[MultiPartitionKeyMultiClusteringKey])
