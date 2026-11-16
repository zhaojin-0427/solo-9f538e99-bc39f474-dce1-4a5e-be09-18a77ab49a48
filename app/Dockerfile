FROM python:3.11-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV APP_DATA=/data APP_PORT=8080
VOLUME /data
EXPOSE 8080
CMD ["python", "app.py"]
