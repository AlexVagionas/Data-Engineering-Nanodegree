import configparser
import boto3
import pandas as pd


def loadConfig():
    '''
    Loads AWS and CLUSTER configurations from dwh.cfg

    '''

    print("---Loading Configurations...---")

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    key = config.get('AWS', 'KEY')
    secret = config.get('AWS', 'SECRET')
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    iam_role_name = config.get('CLUSTER', 'IAM_ROLE_NAME')
    port = config.get('CLUSTER', 'DB_PORT')

    return key, secret, cluster_identifier, iam_role_name, port


def displayIamArn(iam, role_name):
    print("---IAM role ARN---")
    print(iam.get_role(RoleName=role_name)['Role']['Arn'])


def prettyRedshiftProps(props):
    '''
    Returns Redshift cluster properties as pandas DataFrame

    '''

    pd.set_option('display.max_colwidth', -1)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                    "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def getClusterProperties(redshift, cluster_identifier):
    return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]


def displayRedshiftClusterProperties(cluster_props):
    print("---Cluster Properties---")
    print(prettyRedshiftProps(cluster_props))


def displayEndpointARN(cluster_props):
    print("---ENDPOINT and ARN---")
    endpoint = cluster_props['Endpoint']['Address']
    role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
    print("ENDPOINT :: ", endpoint)
    print("ARN :: ", role_arn)


def openTcpPort(ec2, cluster_props, port):
    print("---Opening TCP port...---")
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)


def main():
    # load configurations
    key, secret, cluster_identifier, iam_role_name, port = loadConfig()

    # create client for IAM
    iam = boto3.client('iam',
                       aws_access_key_id=key,
                       aws_secret_access_key=secret,
                       region_name='us-west-2'
                       )

    # create client for Redshift
    redshift = boto3.client('redshift',
                            aws_access_key_id=key,
                            aws_secret_access_key=secret,
                            region_name='us-west-2'
                            )

    # create client for ec2
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=key,
                         aws_secret_access_key=secret
                         )

    displayIamArn(iam, iam_role_name)

    cluster_props = getClusterProperties(redshift, cluster_identifier)

    displayRedshiftClusterProperties(cluster_props)
    displayEndpointARN(cluster_props)

    openTcpPort(ec2, cluster_props, port)


if __name__ == "__main__":
    main()
