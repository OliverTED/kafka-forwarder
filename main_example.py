import os
import asyncio
from server import Cluster, Server

# define first kafka cluster
cluster_edge = Cluster(name="cluster_edge", auth={
    'bootstrap.servers': 'localhost:9092',
    })

# define second kafka cluster
cluster_main = Cluster(name='cluster_main', auth={
    'bootstrap.servers': ['server1:port1','server2:port2'],
    'sasl.plain.username': "username",
    'sasl.plain.password': os.environ['CLUSTER_MAIN_PASSWORD'],
    # 'security.protocol':"SASL_PLAINTEXT",
    # 'sasl.mechanism':"PLAIN",
    })


server = Server()

# forward from one cluster to another
server.forward(cluster_edge, "topic_ingest", default_begin_offset="earliest", group_id="group1", cluster_main, "topic_ingest")

# Forward from one topic to another; (such a use case might also be
# handled with Kafka Streams)
server.forward(cluster_main, "topic_ingest_backup", cluster_main, "topic_ingest")



loop = asyncio.get_event_loop()

loop.run_until_complete(server.run())
