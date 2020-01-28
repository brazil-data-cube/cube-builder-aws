# ===========================================================
#   CREATE VPC
# ===========================================================

# SET ENVS
VPC_NAME="${AWS_PROJECT_NAME}VPC"
VPC_CIDR="172.31.0.0/16"

SUBNET_FIRST_CIDR="172.31.1.0/20"
SUBNET_FIRST_AZ="${AWS_REGION}a"
SUBNET_FIRST_NAME="172.31.1.0 - ${AWS_REGION}a"

SUBNET_SECOND_CIDR="172.31.17.0/20"
SUBNET_SECOND_AZ="${AWS_REGION}b"
SUBNET_SECOND_NAME="172.31.17.0 - ${AWS_REGION}b"

## CREATE VPC
#   start
echo "Creating VPC in preferred region..."
VPC_ID=$(aws --profile $AWS_PROFILE ec2 create-vpc \
  --cidr-block $VPC_CIDR \
  --query 'Vpc.{VpcId:VpcId}' \
  --output text \
  --region $AWS_REGION)
echo "  VPC ID '$VPC_ID' CREATED in '$AWS_REGION' region."

#   add Name tag to VPC
aws --profile $AWS_PROFILE ec2 create-tags \
  --resources $VPC_ID \
  --tags "Key=Name,Value=$VPC_NAME" \
  --region $AWS_REGION
echo "  VPC ID '$VPC_ID' NAMED as '$VPC_NAME'."


## CREATE FIRST SUBNET
#   start
echo "Creating FIRST Subnet..."
SUBNET_FIRST_ID=$(aws --profile $AWS_PROFILE ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block $SUBNET_FIRST_CIDR \
  --availability-zone $SUBNET_FIRST_AZ \
  --query 'Subnet.{SubnetId:SubnetId}' \
  --output text \
  --region $AWS_REGION)
echo "  Subnet ID '$SUBNET_FIRST_ID' CREATED in '$SUBNET_FIRST_AZ'" \
  "Availability Zone."

#   add Name tag to FIRST Subnet
aws --profile $AWS_PROFILE ec2 create-tags \
  --resources $SUBNET_FIRST_ID \
  --tags "Key=Name,Value=$SUBNET_FIRST_NAME" \
  --region $AWS_REGION
echo "  Subnet ID '$SUBNET_FIRST_ID' NAMED as" \
  "'$SUBNET_FIRST_NAME'."


## CREATE SECOND SUBNET
#   start
echo "Creating SECOND Subnet..."
SUBNET_SECOND_ID=$(aws --profile $AWS_PROFILE ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block $SUBNET_SECOND_CIDR \
  --availability-zone $SUBNET_SECOND_AZ \
  --query 'Subnet.{SubnetId:SubnetId}' \
  --output text \
  --region $AWS_REGION)
echo "  Subnet ID '$SUBNET_SECOND_ID' CREATED in '$SUBNET_SECOND_AZ'" \
  "Availability Zone."

#   add Name tag to Public Subnet
aws --profile $AWS_PROFILE ec2 create-tags \
  --resources $SUBNET_SECOND_ID \
  --tags "Key=Name,Value=$SUBNET_SECOND_NAME" \
  --region $AWS_REGION
echo "  Subnet ID '$SUBNET_SECOND_ID' NAMED as '$SUBNET_SECOND_NAME'."


## CREATE GATEWAY
# Create Internet gateway
echo "Creating Internet Gateway..."
IGW_ID=$(aws --profile $AWS_PROFILE ec2 create-internet-gateway \
  --query 'InternetGateway.{InternetGatewayId:InternetGatewayId}' \
  --output text \
  --region $AWS_REGION)
echo "  Internet Gateway ID '$IGW_ID' CREATED."

# Attach Internet gateway to your VPC
aws --profile $AWS_PROFILE ec2 attach-internet-gateway \
  --vpc-id $VPC_ID \
  --internet-gateway-id $IGW_ID \
  --region $AWS_REGION
echo "  Internet Gateway ID '$IGW_ID' ATTACHED to VPC ID '$VPC_ID'."


## CREATE ROUTE
# GET Route Table
ROUTE_TABLE_ID=$(aws --profile $AWS_PROFILE ec2 describe-route-tables \
  --filters=Name=vpc-id,Values=$VPC_ID \
  --query 'RouteTables[*].[RouteTableId]' \
  --output text)
echo "  Route Table ID '$ROUTE_TABLE_ID'."

# Create route to Internet Gateway
RESULT=$(aws --profile $AWS_PROFILE ec2 create-route \
  --route-table-id $ROUTE_TABLE_ID \
  --destination-cidr-block 0.0.0.0/0 \
  --gateway-id $IGW_ID \
  --region $AWS_REGION)
echo "  Route to '0.0.0.0/0' via Internet Gateway ID '$IGW_ID' ADDED to" \
  "Route Table ID '$ROUTE_TABLE_ID'."

# Enable Auto-assign Public IP on Public Subnet
aws --profile $AWS_PROFILE ec2 modify-subnet-attribute \
  --subnet-id $SUBNET_FIRST_ID \
  --map-public-ip-on-launch \
  --region $AWS_REGION
echo "  'Auto-assign Public IP' ENABLED on Public Subnet ID" \
  "'$SUBNET_FIRST_ID'."

aws --profile $AWS_PROFILE ec2 modify-subnet-attribute \
  --subnet-id $SUBNET_SECOND_ID \
  --map-public-ip-on-launch \
  --region $AWS_REGION
echo "  'Auto-assign Public IP' ENABLED on Public Subnet ID" \
  "'$SUBNET_SECOND_ID'."

# enable hostname in VPC
aws --profile $AWS_PROFILE ec2 modify-vpc-attribute \
  --vpc-id $VPC_ID \
  --enable-dns-hostnames

echo "COMPLETED VPC SCRIPT"
echo
. ./create_security_group.sh