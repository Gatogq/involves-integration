from prefect.client import get_client
from prefect.server.schemas.actions import WorkPoolCreate

async def create_work_pool(name:str,description:str,type:str='prefect-agent'):

    async with get_client() as client:

        work_pools = await client.read_work_pools()

        for _ in work_pools:

            if _.name == name:

                return
            
        work_pool = WorkPoolCreate(name=name,description=description,type=type)
        
        response = await client.create_work_pool(work_pool)
        print(response)

import asyncio
asyncio.run(create_work_pool(name='Involves',description='Flujos relacionados a la aplicación Involves',type='process'))
