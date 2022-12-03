FROM python:3.9

WORKDIR /src

COPY /src .

ENV TICKER="BIOS"
ENV AGGREGATES_API_KEY="3iaOPqroAygpgLiKmwH2iNuNg2nqeNk0"
ENV DATABASE_USER="wtnftwsubhbubua"
ENV DATABASE_PASSWORD="tA58agkys3x9udH4VcUY"
ENV DATABASE_HOST="inferno.cdwqdp0jul9p.us-east-1.rds.amazonaws.com"
ENV DATABASE_PORT="3306"
ENV MULTIPLIER="1"
ENV TIMESPAN="day"
ENV ADJUSTED="true"



RUN pip install -r requirements.txt

CMD ["python", "aggregates_etl.py"]