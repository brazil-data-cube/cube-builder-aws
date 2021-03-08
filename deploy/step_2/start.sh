echo
echo 'Set RDS_HOST'
echo 'e.g bdc-test-db.calad2lk.us-east-1.rds.amazonaws.com'
read HOST

echo
echo 'Set RDS_DBNAME'
echo "if you don't know look at 'deploy/step_1/create_rds_db.sh'"
read DBNAME

echo
echo 'Set RDS_USER'
echo "if you don't know look at 'deploy/step_1/create_rds_db.sh'"
read USER

echo
echo 'Set RDS_PASSWORD'
echo "if you don't know look at 'deploy/step_1/create_rds_db.sh'"
read PASSWORD

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
bdc-catalog db init

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
bdc-catalog db init

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
bdc-catalog db create-namespaces

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
bdc-catalog db create-extension-postgis

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
cube-builder-aws alembic upgrade

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
bdc-catalog db create-triggers

SQLALCHEMY_DATABASE_URI="postgresql://${USER}:${PASSWORD}@${HOST}:54320/${DBNAME}" \
cube-builder-aws load-data