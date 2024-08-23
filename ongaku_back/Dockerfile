FROM python:3.12-alpine

WORKDIR /server

COPY requirements.txt /server
RUN pip install -r requirements.txt

RUN ls -lah

ENTRYPOINT [ "fastapi", "dev", "main.py", "--host=0.0.0.0", "--port=8300"]
