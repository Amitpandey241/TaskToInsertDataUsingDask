FROM python:3.10

WORKDIR /app

ADD . /app

RUN pip install -r requirements.txt


CMD ["python","final_task.py"]

