#!/bin/bash
#
# Adapted from https://github.com/DataGrip/docker-env/blob/master/vertica/9.1/docker-entrypoint.sh
#

set -e

# Function to shut down Vertica gracefully
function shut_down() {
  echo "Shutting Down Vertica"
  gosu dbadmin /opt/vertica/bin/admintools -t stop_db -d ${VERTICA_DB} -i
  exit
}

VERTICADATA=${VERTICADATA:-/home/dbadmin/docker}
VERTICA_DB=${VERTICA_DB:-docker}
VERTICA_USER=${VERTICA_USER:-testuser}
VERTICA_PASSWORD=${VERTICA_PASSWORD:-test}

echo "VERTICADATA: ${VERTICADATA}"
echo "VERTICA_DB: ${VERTICA_DB}"
echo "VERTICA_USER: ${VERTICA_USER}"
echo "VERTICA_PASSWORD: ${VERTICA_PASSWORD}"

# Ensure Vertica gets shutdown correctly
trap "shut_down" SIGKILL SIGTERM SIGHUP SIGINT EXIT

# Set dbadmin as the owner of our data
chown -R dbadmin:verticadba "$VERTICADATA"

# If no data exists, create the database, otherwise just start the db
if [ -z "$(ls -A "$VERTICADATA")" ]; then
  echo "Creating database"
  gosu dbadmin /opt/vertica/bin/admintools -t drop_db -d docker
  gosu dbadmin /opt/vertica/bin/admintools -t create_db -s localhost --skip-fs-checks -d $VERTICA_DB -c /home/dbadmin/docker/catalog -D /home/dbadmin/docker/data
  gosu dbadmin /opt/vertica/bin/admintools -t uninstall_package -d docker -P default
else
  gosu dbadmin /opt/vertica/bin/admintools -t start_db -d $VERTICA_DB
fi

if [[ ! -f "${VERTICADATA}/init.sql" ]]; then
cat <<-EOSQL > ${VERTICADATA}/init.sql
CREATE USER $VERTICA_USER IDENTIFIED BY '$VERTICA_PASSWORD';
GRANT ALL ON SCHEMA PUBLIC TO $VERTICA_USER;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA PUBLIC TO $VERTICA_USER;
EOSQL

  /opt/vertica/bin/vsql -h localhost -U dbadmin -d $VERTICA_DB -f ${VERTICADATA}/init.sql
fi

echo "Vertica is now running"

tail -F /home/dbadmin/docker/catalog/docker/v_docker_node0001_catalog/vertica.log

