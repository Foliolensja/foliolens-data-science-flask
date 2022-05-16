web: gunicorn wsgi:app
worker: celery -A app.celery worker -B --loglevel=info