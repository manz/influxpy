FROM influxdb:1.3

COPY ./docker-healthcheck.sh /usr/local/bin/docker-healthcheck.sh
HEALTHCHECK CMD /usr/local/bin/docker-healthcheck.sh
