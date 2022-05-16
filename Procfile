web: gunicorn wsgi:app
worker: celery -A foliolens-data-science worker -l info -B