from celery import Celery
import os
from dotenv import load_dotenv


load_dotenv()



celery_app = Celery(
    'tasks',
    broker='amqp://guest:guest@localhost:5672',
    backend=os.environ.get("DB_URI_LOCAL")
)
