import asyncio

import detloop


async def counter_server(node):
    value = 0

    async def handle(reader, writer):
        nonlocal value
        while line := await reader.readline():
            cmd, *arg = line.split()
            if cmd == b"SET":
                value = int(arg[0])
            writer.write(b"%d\n" % value)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, port=7000)
    await server.serve_forever()


async def increment(host):
    reader, writer = await asyncio.open_connection(host, 7000)
    writer.write(b"GET\n")
    current = int(await reader.readline())
    writer.write(b"SET %d\n" % (current + 1))  # read-modify-write race
    await reader.readline()
    writer.close()


@detloop.simulate(nodes=1, seeds=200)
async def test_concurrent_increments(world):
    world.start(counter_server)
    await asyncio.sleep(0.1)
    await asyncio.gather(increment("n1"), increment("n1"))
    reader, writer = await asyncio.open_connection("n1", 7000)
    writer.write(b"GET\n")
    assert int(await reader.readline()) == 2, "lost update"
    writer.close()
