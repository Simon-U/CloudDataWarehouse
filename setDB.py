import configparser
import boto3
import json
from botocore.exceptions import ClientError

def create_roles(config):
    """
    Discription:    
        Function to set up the different roles (ec2, s3, iam and redshift)
    
    Arguments:
        config: pass the configuration for the Key and Secrete
    Return: 
        touple with the roles
    """
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                    )

    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                   )

    iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                     aws_secret_access_key=config.get('AWS','SECRET'),
                     region_name="us-west-2"
                  )

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                       )
    print("Roles created")
    return [ec2, s3, iam, redshift]

def create_IAM_Role(config, role):
    """
    Discription:    
        Function to set up the IAM_ROLE_ARN
    
    Arguments:
        config: pass the configuration for the Key and Secrete
        roles: the roles from create_roles are here the input
    Return: 
        returns the IAM_role
    """
    iAm = role[2]
    redshift = role[3]
    try:
        Admin_DB = iAm.create_role(
        Path='/',
        RoleName=config.get("IAM_ROLE", "ARN"),
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )   
    except Exception as e:
        print(e)

    iAm.attach_role_policy(RoleName=config.get("IAM_ROLE", "ARN"),
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    print("IAM Role created")

    return iAm.get_role(RoleName=config.get("IAM_ROLE", "ARN"))['Role']['Arn']
 
def create_cluster(config, role, iAmArn):
    """
    Discription:    
        Function to set up the cluster
    
    Arguments:
        config: pass the configuration for the Key and Secrete
        roles: the roles from create_roles are here the input
        iAmArn: the set up IAM_role_ARN
    Return: 
        touple with the roles
    """
    redshift = role[3]
    
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=config.get("CLUSTER","CLUSTER_TYPE"),
            NodeType=config.get("CLUSTER","NODE_TYPE"),
            NumberOfNodes=int(config.get("CLUSTER","NUM_NODES")),

            #Identifiers & Credentials
            DBName=config.get("CLUSTER","DB_NAME"),
            ClusterIdentifier=config.get("CLUSTER","HOST"),
            MasterUsername=config.get("CLUSTER","DB_USER"),
            MasterUserPassword=config.get("CLUSTER","DB_PASSWORD"),

            #Roles (for s3 access)
            IamRoles=[iAmArn]   
        )
    except Exception as e:
        print(e)

    print("Cluster created")
        
def main():
    #reat config file
    conf = configparser.ConfigParser()
    conf.read_file(open('dwh.cfg'))

    roles = create_roles(conf)

    roleArn = create_IAM_Role(conf, roles)

    create_cluster(conf, roles, roleArn)
    
    return roles

if __name__ == "__main__":
    main()