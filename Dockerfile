FROM python:3.10-slim
LABEL maintainer="pete"
#RUN addgroup --gid 1011 --system glow && \
#    adduser  --shell /bin/false --disabled-password --uid 1011 --system --group glow
RUN mkdir -p /app
#RUN chown glow.glow /app
#USER glow
WORKDIR /app
COPY requirements.txt .
#COPY --chown=glow:glow requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir -p ./log
#RUN chown glow.glow ./log
#COPY  --chown=glow getglowdata.py .
COPY  getglowdata.py .
#COPY  --chown=glow glow.py .
COPY  glow.py .
#COPY  --chown=glow glow.env .
#COPY  glow.env .
CMD ["python", "/app/getglowdata.py"]
