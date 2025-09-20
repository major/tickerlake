FROM docker.io/library/python:3.13-slim
RUN pip install datasette
WORKDIR /app
COPY hvcs.db inspect-data.json .
RUN datasette inspect hvcs.db --inspect-file inspect-data.json
EXPOSE 8001
CMD ["datasette", "serve", "hvcs.db", "--port", "8001", "--host", "0.0.0.0", "--inspect-file", "inspect-data.json"]
