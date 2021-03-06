FROM python:latest
RUN pip install gunicorn
RUN pip install Flask
RUN pip install requests
RUN pip install psycopg2
RUN pip install geopy
RUN pip install pyfcm
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apt update -y
RUN apt install -y postgresql

COPY . .
EXPOSE $PORT
CMD sh wait-for-postgres.sh $DATABASE_URL newrelic-admin run-program gunicorn app:app
