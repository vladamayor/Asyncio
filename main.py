import asyncio
import aiohttp
import requests
from more_itertools import chunked

from models import Base, SwPeople, Session, engine
from db import add_data_to_db


def get_people_count():
    response = requests.get('https://swapi.dev/api/people').json()['count']
    return response

async def get_people(client, people_id):
    async with client.get(f"https://swapi.dev/api/people/{people_id}") as response:
        json_data = await response.json()
        return json_data


async def get_id(count):
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client:
        max_requests = 5
        for people_id_chunk in chunked(range(1, count + 1), max_requests):
            people_coros = [get_people(client, people_id)
                            for people_id
                            in people_id_chunk]
            result = await asyncio.gather(*people_coros)
            paste_to_db_coro = add_data_to_db(result)
            paste_to_db_task = asyncio.create_task(paste_to_db_coro)

    tasks = asyncio.all_tasks() - {asyncio.current_task(), }
    for task in tasks:
        await task


if __name__ == '__main__':
    people_count = get_people_count()
    asyncio.run(get_id(people_count))