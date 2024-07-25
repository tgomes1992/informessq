import os





os.system("celery -A InformesLegais worker --pool=threads --concurrency=8")