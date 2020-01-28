# ===========================================================
#   CREATE BUCKET (S3)
# ===========================================================

# CREATE BUCKET (S3)
aws --profile $AWS_PROFILE s3api create-bucket \
    --bucket $AWS_BUCKET_NAME \
    --region $AWS_REGION
echo " create s3 $AWS_BUCKET_NAME"

sleep 5s

# SET PUBLIC-ACCESS TO READ
aws --profile $AWS_PROFILE s3api put-bucket-acl \
    --bucket $AWS_BUCKET_NAME \
    --acl public-read

echo "COMPLETED BUCKET-S3 SCRIPT"
echo
. ./create_vpc.sh