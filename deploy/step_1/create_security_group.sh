# ===========================================================
#   CREATE SECURITY GROUP
# ===========================================================

# SET ENVS
SECURITY_GROUP_NAME="${AWS_PROJECT_NAME}SECURITY"

## CREATE SECURITY GROUP
echo "Creating Security Group"
GROUP_ID=$(aws --profile $AWS_PROFILE ec2 create-security-group \
  --group-name $SECURITY_GROUP_NAME \
  --description "Security group of the cubes generation" \
  --output text \
  --vpc-id $VPC_ID)
echo "  GROUP ID '$GROUP_ID' CREATED"

# authorizations
aws --profile $AWS_PROFILE ec2 authorize-security-group-ingress \
  --group-id $GROUP_ID \
  --protocol tcp --port 5432 --cidr 0.0.0.0/0

echo "COMPLETED SECURITY-GROUP SCRIPT"
echo
. ./create_rds_db.sh
