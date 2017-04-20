#!/usr/bin/env python3
from lib.boto_utils import BotoUtils
from flask import Flask, request, send_from_directory, redirect
from time import sleep
from multiprocessing import Process
from botocore.exceptions import ClientError
import yaml
import json
import sys

def pretty_print(jsonin):
    print(json.dumps(jsonin,indent=4,separators=(',',": ")))

def start_server():
    server = Flask(__name__,static_url_path="")
    import lib.server_routes
    def thread():
        server.run(host='0.0.0.0')
    process = Process(target=thread)
    process.start()
    return process

def stop_server(process):
    process.terminate()
    process.join()

def master_secgroups(instance_id):
    client = get_client_me()
    response = client.describe_instances(InstanceIds=[instance_id])
    instances = response["Reservations"][0]["Instances"]
    sgs = []
    ec2 = get_resource_me()
    for instance in instances:
        for group in instance["SecurityGroups"]:
            sg = ec2.SecurityGroup(group["GroupId"])
            sgs.append(sg)
    for sg in sgs:
        print(sg.ip_permissions)
    return sgs
    
#TODO: ok w/o ipv6?
def blacklist_all_but_ssh(instance_id):
    blacked = 0
    print("removing all ingress except ssh")
    sgs = master_secgroups(instance_id)
    for sg in sgs:
        for permission in sg.ip_permissions:
            if permission['ToPort'] != 22:
                for ip_range in permission["IpRanges"]:
                    print("\trevoking "+permission["IpProtocol"]+" from: "+ip_range["CidrIp"]+" into port:"+str(permission["ToPort"]))
                    sg.revoke_ingress(IpProtocol=permission["IpProtocol"], CidrIp=ip_range["CidrIp"], FromPort=permission["ToPort"], ToPort=permission["ToPort"])
                    blacked += 1
    print("removed permissions for "+str(blacked)+" ingress rules")

def open_5000_for_ip(instance_id,ip):
    sg = master_secgroups(instance_id)[0]
    sg.authorize_ingress(IpProtocol="tcp",CidrIp=(ip+"/32"),FromPort=5000,ToPort=5000)
    print("opened 5000 for "+ip)


def main():

    config = None
    with open("settings.yaml", 'r') as f:
        try:
            config = yaml.load(f)
        except yaml.YAMLError as exc:
            print(exc)
            quit()

    access_key = config["access_key"]

    key_filepath  = "./slave_key.pem"
    key_name = "slave_key"

    connection = BotoUtils(access_key=access_key)
    connection.terminate_slaves()
    connection.delete_key(key_name,key_filepath)
    
if __name__ == "__main__":
    main()
