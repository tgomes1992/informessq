from celery import Celery





celery_app = Celery(
    'tasks',
    broker='amqp://guest:guest@localhost:5672',
    backend='mongodb://localhost:27017/celery_results'
)
