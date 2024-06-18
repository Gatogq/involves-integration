from prefect import flow
from prefect.runner.storage import GitRepository


if __name__ == '__main__':
    flow.from_source(source=GitRepository(url='https://github.com/Gatogq/involves-integration.git'),
                        entrypoint="flows.py:update_involves_clinical_db",

                        ).deploy(
                            name='actualizar_base_involves_clinical_sql',
                            work_pool_name='involves-env',
                            build=False,
                            
                        )
    

