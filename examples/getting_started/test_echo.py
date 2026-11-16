import asyncio
import detloop

async def echo_server(node):
    async def handle(reader, writer):
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()
        writer.close()
    server = await asyncio.start_server(handle, port=9000)
    await server.serve_forever()

@detloop.simulate(nodes=2, seeds=50, faults="partitions")
async def test_echo(world):
    world.start(echo_server, nodes=["n1"])     # run echo_server(node) on n1
    await asyncio.sleep(0.1)                   # virtual time: instant
    reader, writer = await asyncio.open_connection("n1", 9000)
    writer.write(b"ping")
    assert await reader.readexactly(4) == b"ping"
    writer.close()
