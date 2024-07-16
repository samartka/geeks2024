FROM python:3.12-slim

WORKDIR /flu_id_cronjob

ADD . /flu_id_cronjob

RUN apt-get update
RUN apt-get install -y cron

COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

RUN pip install --no-cache-dir -r requirements.txt

CMD ["cron", "-f"]
