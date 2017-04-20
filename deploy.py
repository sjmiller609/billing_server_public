#!/usr/bin/env python3
from lib.boto_utils import BotoUtils
from flask import Flask, request, send_from_directory, redirect
from time import sleep
from multiprocessing import Process
from botocore.exceptions import ClientError
import requests
from subprocess import check_output
import subprocess
import shutil
import re
import yaml
import json
import os
import xlrd
import sys

#removes whitespace from elements
def strip_json(tree,depth=0):
    if isinstance(tree, str):
        tree = tree.strip()
    elif isinstance(tree,list):
        for elem in tree:
            elem = strip_json(elem,depth=depth+1)
    elif isinstance(tree,dict):
        for key in tree:
            tree[key] = strip_json(tree[key],depth=depth+1)
    return tree

def get_billing_profiles(filename):
    xl_workbook = xlrd.open_workbook(filename)
    sheet_names = xl_workbook.sheet_names()
    print('Sheet Names', sheet_names)
    xl_sheet = xl_workbook.sheet_by_name(sheet_names[0])
    xl_sheet = xl_workbook.sheet_by_index(0)
    row = xl_sheet.row(0)  # 1st row
    print(row)
    results = {}
    for i in range(1,xl_sheet.nrows):
        print(xl_sheet.row(i))
        json_cell = json.loads(xl_sheet.cell_value(i,xl_sheet.ncols-1))
        key = None
        for key_ in json_cell:
            results[key_] = json_cell[key_]
    strip_json(results)
    pretty_print(results)
    print("found "+str(len(results))+" billing profiles")
    return results

def pretty_print(jsonin):
    print(json.dumps(jsonin,indent=4,separators=(',',": ")))

def stop_server(process):
    process.terminate()
    process.join()

def master_secgroups(connection,instance_id):
    client = connection.client
    response = client.describe_instances(InstanceIds=[instance_id])
    instances = response["Reservations"][0]["Instances"]
    sgs = []
    ec2 = connection.resource
    for instance in instances:
        for group in instance["SecurityGroups"]:
            sg = ec2.SecurityGroup(group["GroupId"])
            sgs.append(sg)
    '''
    for sg in sgs:
        print(sg.ip_permissions)
    '''
    return sgs
    
#TODO: also blacklist ipv6
def blacklist_all_but_ssh(connection,instance_id):
    blacked = 0
    print("removing all ingress except ssh")
    sgs = master_secgroups(connection,instance_id)
    for sg in sgs:
        for permission in sg.ip_permissions:
            if permission['ToPort'] != 22:
                for ip_range in permission["IpRanges"]:
                    #print("\trevoking "+permission["IpProtocol"]+" from: "+ip_range["CidrIp"]+" into port:"+str(permission["ToPort"]))
                    sg.revoke_ingress(IpProtocol=permission["IpProtocol"], CidrIp=ip_range["CidrIp"], FromPort=permission["ToPort"], ToPort=permission["ToPort"])
                    blacked += 1
    print("removed permissions for "+str(blacked)+" ingress rules")

def open_port_for_ips(connection,instance_id,port,ips):
    for ip in ips:
        sg = master_secgroups(connection,instance_id)[0]
        sg.authorize_ingress(IpProtocol="tcp",CidrIp=(ip+"/32"),FromPort=port,ToPort=port)
        print("opened "+str(port)+" for "+ip)
    print("opened port "+str(port)+" for the "+str(len(ips))+" chosen hosts")

def load_config(config_file):
    with open(config_file, 'r') as f:
        try:
            config = yaml.load(f)
            return config
        except yaml.YAMLError as exc:
            print(exc)
            quit()

def get_local_instance_id():
    response = requests.get("http://169.254.169.254/latest/meta-data/instance-id")
    if "200" not in str(response):
        print("ERROR: querying local instance id")
        print(response)
        print(response.text)
        quit()
    re_id = re.compile("i-[a-z0-9]*")
    if not re_id.match(response.text):
        print("ERROR: did not get an id when querying for ours.")
        print(response)
        print(response.text)
        quit()
    return response.text
    
def wait_for_results(connection,id_ip_map,key_filepath,output_filepath):
    re_ipv4 = re.compile("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$")
    ids_done = []
    while len(ids_done) < len(id_ip_map):
        sleep(10)
        for id_ in id_ip_map:
            if id_ not in ids_done:
                ip = id_ip_map[id_]
                if not re_ipv4.match(ip):
                    print("WOAH!, we expected to get an ipv4 address here.")
                    print(ip)
                    quit()
                command = ["ssh","-q","-oStrictHostKeyChecking=no","-i",key_filepath,"ubuntu@"+ip,"ls",output_filepath]
                output=None
                try:
                    output = str(check_output(command))
                except subprocess.CalledProcessError as e:
                    pass
                except Exception as e:
                    print("WARNING: there was an error calling checkoutput.")
                    print(e)

                if output is not None and output_filepath not in output:
                        print("WARNING: expected to see "+output_filepath+" in the reponse from ssh.")
                        print(output)
                if output is not None and output_filepath in output and "No such" not in output:
                    print("we detected results from "+id_+":"+ip+" ! downloading...")
                    output_path = "./results/"+id_
                    try:
                        os.remove(output_path)
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        print("while trying to delete after detecting")
                        print(e)
                    command = ["scp","-q","-oStrictHostKeyChecking=no","-i",key_filepath,"ubuntu@"+ip+":"+output_filepath,output_path]
                    try:
                        check_output(command)
                    except subprocess.CalledProcessError as e:
                        pass
                    except Exception as e:
                        print("WARNING: check_output threw and exception")
                        print(command)
                        print(e)
                    if not os.path.isfile(output_path):
                        print("!!!! WARNING: failed to download results from "+ip)
                    else:
                        print("result logged in "+output_path)
                        print("terminating instance. "+id_)
                        connection.terminate_id(id_)
                        ids_done.append(id_)

def create_billing_file(filename,billing_profile,directory,keywords,drop_link,category):
    json_in = json.loads(json.dumps(billing_profile))
    json_in["keywords"] = keywords
    json_in["drop_link"] = drop_link
    json_in["category"] = category
    with open(directory+filename,"w") as f:
        f.write(json.dumps(json_in))

def create_billing_dir(profiles,ids,billing_path,keywords,drop_link,category):
    try:
        shutil.rmtree(billing_path)
    except FileNotFoundError:
        pass
    os.mkdir(billing_path)
    if len(ids) != len(profiles):
        print("WARING: not the same number of profiles and ids")
        print(str(len(ids))+" ids")
        print(str(len(profiles))+" profiles")
    iterations = len(ids)
    if len(profiles) < iterations: iterations = len(profiles)
    keys = []
    for key in profiles:
        keys.append(key)
    for i in range(0,iterations):
        create_billing_file(ids[i],profiles[keys[i]],billing_path,keywords,drop_link,category)
            
def start_server(billing_path,port):
    server = Flask(__name__,static_url_path="")
    @server.route('/billing/<id_input>')
    def send_billing_info(id_input):
        print("got request to /billing/"+id_input)
        return send_from_directory("billing",id_input)
    process = Process(target=server.run,args=("0.0.0.0","5001",))
    process.start()
    return process


def main():


    config = load_config("settings.yaml")
    slave_instance_id = config["slave_instance_id"]
    access_key = config["access_key"]
    billing_excel = config["billing_info_excel"]
    output_filepath = config["output_filepath"]
    startup_script = config["startup_script"]
    billing_path = config["billing_path"]
    item_keywords = config["item_keywords"]
    drop_link = config["drop_link"]
    category = config["category"]

    our_instance_id = get_local_instance_id()
    key_filepath  = "./slave_key.pem"
    key_name = "slave_key"
    server_port = 5001
    slave_image_name = 'slave_image'
    serveronly = "serveronly" in sys.argv
    
    billing_profiles = get_billing_profiles(billing_excel)
    count = len(billing_profiles)

    connection = BotoUtils(access_key=access_key)
    blacklist_all_but_ssh(connection,our_instance_id)
    ids = None
    if not serveronly:
        print("creating an ami with which we will spawn slaves")
        slave_image_id = connection.create_ami(slave_instance_id,slave_image_name,'for automation')
        try:
            connection.new_key(key_name,key_filepath)
        except ClientError as e:#TODO: specify
            connection.delete_key(key_name,key_filepath)
            connection.new_key(key_name,key_filepath)

        ids = connection.start_instances(slave_image_id,count,["ssh-globally"],"t2.micro",key_name,startup_script)
        connection.deregister_image_name(slave_image_name)
    else:
        ids = ["TODO"]
    print("creating directory for billing server")
    create_billing_dir(billing_profiles,ids,billing_path,item_keywords,drop_link,category)
    process = start_server(billing_path,server_port)
    id_ip_map = connection.get_ips(ids)
    ips = []
    for key in id_ip_map:
        ips.append(id_ip_map[key])
    open_port_for_ips(connection,our_instance_id,server_port,ips)
    
    #this will terminate instances after we get results
    print("waiting for results")
    if not serveronly:
        wait_for_results(connection,id_ip_map,key_filepath,output_filepath)
    else:
        input("press enter to end")
    stop_server(process)
    blacklist_all_but_ssh(connection,our_instance_id)
    connection.terminate_slaves()
    connection.delete_key(key_name,key_filepath)
    
if __name__ == "__main__":
    main()
