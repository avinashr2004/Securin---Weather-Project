import os
from sqlalchemy import create_engine

# Get the exact location of THIS file on your computer
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Force the database to be created in the same folde
DB_PATH = os.path.join(BASE_DIR, "weather.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create connection
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})