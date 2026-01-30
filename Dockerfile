FROM kestra/kestra:latest

RUN mkdir -p /app/plugins

COPY build/libs/* /app/plugins/