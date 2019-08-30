import asyncio
import heapq
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition



class Cluster(object):
    def __init__(self, name, auth, enable_idempotence=False):
        self.name = name
        self.auth = auth
        self.producer = None
        self.producer_wait = None
        self.enable_idempotence = enable_idempotence

    async def get_producer(self):
        if self.producer is None:
            loop = asyncio.get_running_loop()
            auth = {k.replace('.', '_'):v for k,v in self.auth.items()}

            self.producer = AIOKafkaProducer(loop=loop, **auth, enable_idempotence=self.enable_idempotence)

            try:
                await self.producer.start()
            except:
                await self.producer.stop()

        return self.producer

    async def create_consumer(self, topic, default_begin_offset, group_id):
        loop = asyncio.get_running_loop()
        auth = {k.replace('.', '_'):v for k,v in self.auth.items()}

        consumer = AIOKafkaConsumer(topic, loop=loop, **auth, group_id=group_id, enable_auto_commit=False, auto_offset_reset=default_begin_offset)

        await consumer.start()

        return consumer


async def _forward(cluster_from, topic_from, cluster_to, topic_to, default_begin_offset, group_id, buffer_size, commit_every_sec=5):
    # TODO add repartitioning_strategy="strict_p2p",

    todo = asyncio.Queue(buffer_size)
    loop = asyncio.get_running_loop()

    _offset_heaps = {}
    def get_heap(p):
        res = _offset_heaps.get(p, [])
        _offset_heaps[p] = res
        return res

    async def loop_func(func):
        try:
            while True:
                await func()
        finally:
            try:
                await consumer.stop()
            except:
                pass
            try:
                await producer.stop()
            except:
                pass

    async def _consumer(consumer):
            async for msg in consumer:
                await todo.put(msg)
                print(f'got {msg}')


    async def _send(msg):
        try:
            await producer.send_and_wait(topic_to, msg.value, msg.key, msg.partition, msg.timestamp)
            print(f'sent {msg} -> {topic_to}')
            heap = get_heap(msg.partition)
            heapq.heappush(heap, msg.offset)
        except:
            await todo.put(msg)

    async def _producer(producer):
        msg = await todo.get()
        loop.create_task(_send(msg))

    new_offsets = {}

    async def _committer(consumer):
        await asyncio.sleep(commit_every_sec)

        ps = consumer.partitions_for_topic(topic_from) or []

        last_offsets = {
            p: await consumer.committed(
                TopicPartition(topic_from, p)) or -1
                for p in ps}

        for p in ps:
            last_offset = last_offsets[p]
            new_offset = new_offsets.get(p, last_offset)

            heap = get_heap(p)
            while len(heap)>0 and heap[0] <= new_offset:
                new_offset=max([heap[0]+1, new_offset])
                heapq.heappop(heap)

            new_offsets[p] = new_offset

        for p in ps:
            last_offset = last_offsets[p]
            new_offset = new_offsets[p]

            if new_offset > last_offset:
                await consumer.commit({
                    TopicPartition(topic_from, p):new_offset
                })
                print(f'committed ({topic_from}/{group_id},{p}:{last_offset}->{new_offset})')

    consumer = None
    producer = None

    try:
        consumer = await cluster_from.create_consumer(topic_from, default_begin_offset, group_id)
        producer = await cluster_to.get_producer()

        return await asyncio.gather(
            loop_func(lambda:_consumer(consumer)),
            loop_func(lambda:_producer(producer)),
            loop_func(lambda:_committer(consumer)),
            )

    except Exception as e:
        raise e

    finally:
        print("done")
        if consumer is not None:
            try:
                await consumer.stop()
            except:
                pass

        if producer is not None:
            try:
                await producer.stop()
            except:
                pass



class Server(object):
    def __init__(self):
        self.tasks = []


    def forward(self, cluster_from, topic_from, cluster_to, topic_to,
                 default_begin_offset="earliest", group_id=None, buffer_size=1000):

        # TODO add repartitioning_strategy="strict_p2p",
        if group_id is None:
            group_id = f"_forward_{topic_from}_{cluster_to.name}_{topic_to}"
        task = _forward(cluster_from, topic_from, cluster_to, topic_to,
                        default_begin_offset, group_id, buffer_size)
        self.tasks.append(task)

    async def run(self):
        await asyncio.gather(*self.tasks)
