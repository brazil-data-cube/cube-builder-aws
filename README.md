# AWS - Generate Cubes (BDC Datastorm)

### **CONFIGURE**:

1)  in AWS Console

    - create AWS account
    - Loggin with AWS account created
    - create IAM user
    - set full permissions (fullAccess) to IAM user created
    - generate credentals to IAM user

2) in your HOST (command line):

    - install *AWS CLI*
    - configure credentials
        -  e.g: aws configure --profile *iam-user-name*
    - install *Node.js* (global)
    - install *serverless*

-----

### **RUNNNIG**:

```
$ cd deploy/step_1/
$ sh start.sh

** access https://console.aws.amazon.com/rds/home by browser
** select region used to create RDS
** select databases
** Wait until the created database has a status of 'Available' (~10min)
** click on database

$ cd ../../deploy/step_2/
$ sh start.sh

** set environment with your information in *../../bdc-scripts/serverless.yml*

$ cd ../../deploy/step_3/
$ sh deploy.sh
```

### **ENDPOINTS**:

```
GET STATUS:
curl {endpoint}/

1) Create `grs_schema`
    - {endpoint}/create-grs
    - POST

2) Create `raster_size_schema`
    - {endpoint}/create-grs
    - POST

3) Create `cube`
    - {endpoint}/create-raster-size
    - POST

4) Start `process` to generate image of cube
    - {endpoint}/start
    - GET

```

- [`SPEC`](https://github.com/betonr/bdc-scripts-aws/tree/master/spec) (OpenAPI 3.0)