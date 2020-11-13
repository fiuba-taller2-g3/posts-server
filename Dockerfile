FROM python:latest
COPY . .
RUN pip install gunicorn
RUN pip install Flask
RUN pip install requests
RUN apt update -y
RUN apt install -y postgresql
EXPOSE $PORT
CMD gunicorn app:app
