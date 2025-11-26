FROM kestra/kestra:latest-no-plugins

RUN mkdir -p /app/plugins

COPY build/libs/* /app/plugins/