import json
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import as_declarative

path_to_config = os.environ.get("PATH_TO_CONFIG", "config.json")

with open(path_to_config, 'r') as f:
    config = json.load(f)

db_user = config["database"]["username"]
db_host = config["database"]["host"]
db_password = config["database"]["password"]
db_database = config["database"]["name"]

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_database}")



@as_declarative()
class AbstractModel: pass


