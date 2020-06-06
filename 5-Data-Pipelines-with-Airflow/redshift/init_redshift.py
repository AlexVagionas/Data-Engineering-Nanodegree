import configparser
import boto3

# load configurations
config = configparser.ConfigParser()
config.read('redshift.cfg')

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

# create client for Redshift
redshift = boto3.client('redshift',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                        )

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
    )
except Exception as e:
    print(e)
