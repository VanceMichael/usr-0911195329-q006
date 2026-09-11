FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN python -m unittest -v
EXPOSE 8080
CMD ["python","app.py"]
