import asyncio
from importlib import import_module

pytest = import_module("pytest")


@pytest.mark.asyncio
async def test_prepare_can_download_b_while_a_send_blocked_and_commit_order_holds():
    source = asyncio.Queue()
    prepared = asyncio.Queue(maxsize=4)
    downloaded = []
    committed = []
    release_a = asyncio.Event()

    async def prepare():
        for _ in range(2):
            item = await source.get()
            downloaded.append(item)
            await prepared.put(item)
            source.task_done()

    async def send():
        for _ in range(2):
            item = await prepared.get()
            if item == "A": await release_a.wait()
            committed.append(item)
            prepared.task_done()

    await source.put("A"); await source.put("B")
    prepare_task = asyncio.create_task(prepare())
    send_task = asyncio.create_task(send())
    await source.join()
    assert downloaded == ["A", "B"] and committed == []
    release_a.set()
    await asyncio.gather(prepare_task, send_task)
    assert committed == ["A", "B"]
