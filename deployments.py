from prefect import flow
from prefect.runner.storage import GitRepository


if __name__ == '__main__':
    flow.from_source(source=GitRepository(url='https://github.com/Gatogq/involves-integration.git'),
                        entrypoint="flows.py:update_involves_clinical",

                        ).deploy(
                            name='update_involves_db_from_repo',
                            work_pool_name='Involves',
                            build=False
                        )