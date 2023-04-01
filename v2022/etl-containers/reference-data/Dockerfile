FROM python:3.9

WORKDIR /src

COPY /src .

RUN pip install -r requirements.txt

CMD ["python", "reference_data_etl.py"]