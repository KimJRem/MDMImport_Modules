FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN ["chmod", "+x", "./wait-for-it.sh"]

CMD [ "./wait-for-it.sh", "rabbitmq:5672", "--", "python", "./TrafficXmlToJsonNew.py" ]
