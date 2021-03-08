#######################################################
# START PROCESS - MODULE 1 
#######################################################

echo
echo 'Set profile:'
echo 'obs: create with aws configure (.aws/credentials)'
read AWS_PROFILE

echo
echo 'Set region:'
echo 'e.g us-east-1'
read AWS_REGION

echo
echo 'Set project name:'
echo 'obs: one word - all tiny'
read AWS_PROJECT_NAME

echo
echo 'Set bucket name:'
echo 'obs: a bucket with this name cannot exist'
read AWS_BUCKET_NAME

. ./create_bucket.sh