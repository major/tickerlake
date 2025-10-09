FROM docker.io/library/python:3.14-slim@sha256:1e7c3510ceb3d6ebb499c86e1c418b95cb4e5e2f682f8e195069f470135f8d51
RUN pip install datasette
WORKDIR /app
COPY hvcs.db inspect-data.json .
RUN datasette inspect hvcs.db --inspect-file inspect-data.json
EXPOSE 8001
CMD ["datasette", "serve", "hvcs.db", "--port", "8001", "--host", "0.0.0.0", "--inspect-file", "inspect-data.json"]
