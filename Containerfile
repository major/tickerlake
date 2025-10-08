FROM docker.io/library/python:3.14-slim@sha256:9dc4ef3e628432af2237d1418908f5c6d4528e9f776aaa6e7c95c18854c86e48
RUN pip install datasette
WORKDIR /app
COPY hvcs.db inspect-data.json .
RUN datasette inspect hvcs.db --inspect-file inspect-data.json
EXPOSE 8001
CMD ["datasette", "serve", "hvcs.db", "--port", "8001", "--host", "0.0.0.0", "--inspect-file", "inspect-data.json"]
