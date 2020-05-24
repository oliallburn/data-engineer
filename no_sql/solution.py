from __future__ import print_function
import aerospike
import logging
from aerospike import exception as ex
import sys
from aerospike import predicates as p


logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

namespace = 'test'
set = 'otus_hw'
phone_number_index = 'otus_phone_number_idx'

phone_n = 'phone_number'
lft = 'lifetime_value'

config = {
    'hosts': [
        ( 'db', 3000 )
    ],
    'policies': {
        'total_timeout': 1000 
    }
}

def make_aerospike_client(config):
    try:
        return aerospike.client(config).connect() 
    except ex.AerospikeError as e:
        logging.error('Failed to connect to the cluster with', config['hosts'])
        sys.exit(1)

def add_customer(customer_id, phone_number, lifetime_value):
    key = (namespace, set, customer_id)
    attr = {phone_n: phone_number, lft: lifetime_value}
    try:
        client.put(key, attr)
    except ex.AerospikeError as e:
        logging.error('Failed to connect to the cluster with', config['hosts'])
        sys.exit(1)


def get_ltv_by_id(customer_id):
    key = (namespace, set, customer_id)
    try:
        (key, metadata, record) = client.get(key)
        return record[lft]
    except ex.RecordNotFound:
        logging.error("Record not found:", key)    
    except ex.AerospikeError as e:
        logging.error('Failed to connect to the cluster with', config['hosts'])
        sys.exit(1)


def get_ltv_by_phone(phone_number):
    query = client.query(namespace, set) \
       .select(phone_n, lft) \
       .where(p.equals(phone_n, phone_number))
    try:
        results = query.results()
    except ex.IndexNotFound as e:
        logging.error('There is no secondary index defined for phone_number')
        return None
    if results:
        return results[0][2].get(lft)
      
client = make_aerospike_client(config)
client.index_integer_create(namespace, set, phone_n, phone_number_index)

# try:
#     client.index_integer_create(namespace, set, phone_n, phone_number_index)
# except ex.IndexFoundError as e:
#     logging.error('Error: {0} [{1}]'.format(e.msg, e.code))   

for i in range(0, 1000):
    add_customer(i, i, i + 1)

for i in range(0, 1000):
    assert (i + 1 == get_ltv_by_id(i)), 'No LTV by ID ' + str(i)
    assert (i + 1 == get_ltv_by_phone(i)), 'No LTV by phone ' + str(i)

client.index_remove(namespace, phone_number_index)
client.close()