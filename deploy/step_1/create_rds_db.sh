# ===========================================================
#   CREATE RDS (Postgres)
# ===========================================================

# SET ENVS
AWS_REGION="${AWS_REGION}b"
RDS_NAME="bdc"
RDS_ID="${AWS_PROJECT_NAME}-db"


echo
echo 'Set RDS user:'
read RDS_USER

echo
echo 'Set RDS password:'
read RDS_PASSWORD


SUBNET_GROUP_NAME="${AWS_PROJECT_NAME}SubnetGroup"

# CREATE SUBNET GROUP
SUBNET_GROUP=$(aws --profile $AWS_PROFILE rds create-db-subnet-group \
    --db-subnet-group-name $SUBNET_GROUP_NAME \
    --db-subnet-group-description "subnet group cube generation" \
    --output text \
    --subnet-ids $SUBNET_FIRST_ID $SUBNET_SECOND_ID)
echo " create db sub $SUBNET_GROUP_NAME"

# CREATE DB (RDS)
aws --profile $AWS_PROFILE rds create-db-instance \
    --db-name $RDS_NAME \
    --db-instance-identifier $RDS_ID \
    --allocated-storage 20 \
    --db-instance-class db.t2.micro \
    --engine postgres \
    --master-username $RDS_USER \
    --master-user-password $RDS_PASSWORD \
    --publicly-accessible \
    --vpc-security-group-ids $GROUP_ID \
    --db-subnet-group-name $SUBNET_GROUP_NAME \
    --availability-zone $AWS_REGION \
    --port 5432

echo "COMPLETED RDS SCRIPT"

echo
echo "PROCESS COMPLETED"
echo