from prefect.blocks.system import JSON
from dotenv import dotenv_values

# Load environment variables from .env file
env_vars = dotenv_values('config/.env')
print(env_vars)

json_block = JSON(value=env_vars)

json_block.save(name="involves-clinical-env-vars")