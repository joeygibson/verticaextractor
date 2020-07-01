FROM datagrip/vertica:9.1.1

# Adapted from https://github.com/DataGrip/docker-env/blob/master/vertica/9.1/docker-entrypoint.sh

COPY ./docker-entrypoint.sh /opt/vertica/bin/

RUN chmod +x /opt/vertica/bin/docker-entrypoint.sh

ENTRYPOINT ["/opt/vertica/bin/docker-entrypoint.sh"]
EXPOSE 5433

