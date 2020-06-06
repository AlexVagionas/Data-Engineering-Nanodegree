import configparser
import boto3


def loadConfig():
    """
    Loads AWS and CLUSTER configurations from dwh.cfg

    """

    print("---Loading Configurations...---")

    config = configparser.ConfigParser()
    config.read('redshift.cfg')

    key = config.get('AWS', 'KEY')
    secret = config.get('AWS', 'SECRET')
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    return key, secret, cluster_identifier


def main():
    # load configurations
    key, secret, cluster_identifier = loadConfig()

    # create client for Redshift
    redshift = boto3.client('redshift',
                            aws_access_key_id=key,
                            aws_secret_access_key=secret,
                            region_name='us-west-2'
                            )

    print("---Deleting Cluster...---")
    redshift.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)


if __name__ == "__main__":
    main()
