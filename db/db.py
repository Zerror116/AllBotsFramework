
import json
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

path_to_config = os.environ.get("PATH_TO_CONFIG", "config.json")

with open(path_to_config, 'r') as f:
    config = json.load(f)

db_user = config["database"]["username"]
db_host = config["database"]["host"]
db_password = config["database"]["password"]
db_database = config["database"]["name"]

DATABASE_URL = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_database}"

# Защита от "мертвых" соединений и периодическая переработка пула
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args={}  # при необходимости добавьте sslmode или другие параметры
)

# Session factory
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Базовый класс для моделей
Base = declarative_base()