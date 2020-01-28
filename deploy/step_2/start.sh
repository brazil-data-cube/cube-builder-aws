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

docker rmi bdc_start_db
cd generate-db
docker build -t bdc_start_db .

docker rm bdc_start_db_container
docker run --name bdc_start_db_container \
    -e RDS_HOST=${HOST} \
    -e RDS_DBNAME=${DBNAME} \
    -e RDS_USER=${USER} \
    -e RDS_PASSWORD=${PASSWORD} bdc_start_db
    
docker stop bdc_start_db_container
docker rm bdc_start_db_container
docker rmi bdc_start_db
cd ..

echo
echo 'RESTORE COMPLETED'
