import configparser
import pandas as pd
import json
import boto3

# load configurations
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
NUM_NODES = config.get('CLUSTER', 'NUM_NODES')
NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')
IAM_ROLE_NAME = config.get('CLUSTER', 'IAM_ROLE_NAME')

# display configurations
df = pd.DataFrame({'Param': ['CLUSTER_TYPE', 'NUM_NODES', 'NODE_TYPE', 'CLUSTER_IDENTIFIER', 'DB_NAME',
                             'DB_USER', 'DB_PASSWORD', 'DB_PORT', 'IAM_ROLE_NAME'],
                   'Values': [CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER, DB_NAME,
                              DB_USER, DB_PASSWORD, DB_PORT, IAM_ROLE_NAME]
                   })
print(df)

# create client for IAM
iam = boto3.client('iam',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                   )

# create client for Redshift
redshift = boto3.client('redshift',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                        )

# create IAM role
try:
    print("---Creating a new IAM Role---")
    dwhRole = iam.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )
except Exception as e:
    print(e)

# attach S3 read only access to IAM role
print("---Attaching Policy---")

iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                       )['ResponseMetadata']['HTTPStatusCode']

roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

# create Redshift cluster
try:
    print("---Creating a new Redshift Cluster---")
    response = redshift.create_cluster(
        # HW
        ClusterType=CLUSTER_TYPE,
        NodeType=NODE_TYPE,
        NumberOfNodes=int(NUM_NODES),

        #Identifiers & Credentials
        DBName=DB_NAME,
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,

        # Roles (for s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)
