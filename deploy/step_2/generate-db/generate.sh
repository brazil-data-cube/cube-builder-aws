rm -r ./bdc-db
rm -r ./migrations

git clone https://github.com/brazil-data-cube/bdc-db.git

cd bdc-db && git checkout b-0.2

cd ../

mv ./bdc-db/migrations ./

rm -r ./bdc-db

python ./manage.py create_extension

python ./manage.py db upgrade

# UPDATE TEMPORAL COMPOSITES
PGPASSWORD=${RDS_PASSWORD} psql -U ${RDS_USER} -h ${RDS_HOST} ${RDS_DBNAME} < ./data/temporal_composite.sql
# UPDATE COMPOSITE FUNCTIONS
PGPASSWORD=${RDS_PASSWORD} psql -U ${RDS_USER} -h ${RDS_HOST} ${RDS_DBNAME} < ./data/composite_function.sql
