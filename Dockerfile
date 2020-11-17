FROM python:latest
RUN pip install gunicorn
RUN pip install Flask
RUN pip install requests
RUN pip install psycopg2
RUN apt update -y
RUN apt install -y postgresql

COPY . .
EXPOSE $PORT
CMD sh wait-for-postgres.sh $DATABASE_URL gunicorn app:app
