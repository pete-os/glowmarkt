FROM python:3.10
LABEL maintainer="pete"
WORKDIR /usr/glowmarkt/src

ADD getglowdata.py .
ADD GlowClient.py .
ADD glow.checkpoint .
ADD glow.env .
RUN pip install python-dotenv python-dateutil paho-mqtt pytz requests
CMD ["python", "./getglowdata.py"]