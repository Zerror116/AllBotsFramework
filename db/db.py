import json
import os
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import as_declarative

try:
    path_to_config = os.environ.get("PATH_TO_CONFIG", "config.json")

    with open(path_to_config, 'r') as f:
        config = json.load(f)

    db_user = config["database"]["username"]
    db_host = config["database"]["host"]
    db_password = config["database"]["password"]
    db_database = config["database"]["name"]

    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_database}"
    )

    @as_declarative()
    class AbstractModel:
        pass

except Exception:
    print("Ошибка при инициализации базы данных:")
    print(traceback.format_exc())
