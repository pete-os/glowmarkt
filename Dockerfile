FROM python:3.10
LABEL maintainer="pete"
WORKDIR /usr/glowmarkt/src

RUN mkdir -p ./log
ADD getglowdata.py .
ADD glow.py .
#ADD glow.checkpoint .
ADD glow.env .
RUN pip install python-dotenv python-dateutil paho-mqtt pytz requests
CMD ["python", "./getglowdata.py"]