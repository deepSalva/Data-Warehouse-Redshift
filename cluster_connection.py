"""
This script contain the necessary resources for the creating of the
Redshift cluster. It uses the 'boto3' framework to create the client
to connect with the server. The relevant information to create the client
and the cluster allocates in the 'dhh.cfg' script.

To change the rol, the user or the endpoint for the connection refer to the
'dhh.cfg' script.
"""

import pandas as pd
import boto3
import json
import configparser
import time
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# creates the client
iam = boto3.client('iam',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                   )

# creates redshift cluster client
redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

# creates the EC2 client
ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )


def cluster_prop():
    """return a variable that reads the cluster status"""

    return redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]


def create_rol():
    """Creates the rol for the cluster in case it is not created already"""
    # 1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)

    return roleArn


def create_cluster(roleArn):
    """Launch the cluster"""

    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    """return a pandas data frame with the cluster state information useful to check connectivity"""

    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def is_connected():
    """Checks the whether the cluster is active or not. This function pause the process until the
    cluster is active"""

    available = prettyRedshiftProps(cluster_prop())._get_value(2, 'Value')
    while available != "available":
        print("Creatting the cluster...")
        available = prettyRedshiftProps(cluster_prop())._get_value(2, 'Value')
        time.sleep(50)
    print('Redshift cluster created!!')


def endpoint_and_key():
    """DO NOT RUN THIS unless the cluster status becomes "Available". Make ure you are checking your Amazon
     Redshift cluster in the us-west-2 region. """
    try:
        myClusterProps = cluster_prop()
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    except Exception as e:
        print("User already created")


def open_tcp():
    """Opens a tcp port if necessary, once the cluster is created. This function is not used by default"""

    myClusterProps = cluster_prop()
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print("Endpoint already exist")


def cluster_connect():
    """Executes in order the function for the cluster creation"""

    role_arn = create_rol()
    create_cluster(role_arn)
    is_connected()
    endpoint_and_key()
    open_tcp()
    # open_tcp()


def cluster_disconnect():
    """WARNING this function clear all resources from the Readshift cluster
    This function is activated in the 'clear_resources.py' script"""

    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    deleting = prettyRedshiftProps(cluster_prop())._get_value(2, 'Value')
    while deleting == "deleting":
        print("Clearing cluster resources...")
        try:
            deleting = prettyRedshiftProps(cluster_prop())._get_value(2, 'Value')
            time.sleep(30)
        except Exception as e:
            print("++Cluster cleared++")
            iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                                   PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
            iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
            print("++IAM rol deleted++")
            break
