FROM python:3.9

WORKDIR /src

COPY /src .

ENV DEBUG_LEVEL INFO

RUN pip install -r requirements.txt

CMD ["python", "main.py"]