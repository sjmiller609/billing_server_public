import boto3
from getpass import getpass
from time import sleep
import os
import re
import socket
import sys
from botocore.exceptions import ClientError
from random import randint

class BotoUtils():

    def __init__(self,access_key=None,secret_key=None):
        if access_key == None:
            access_key = input("please enter your AWS access key id: ")
        if secret_key == None:
            secret_key = getpass("please enter your AWS secret key (no visual feedback): ")
        self.aws_access_key_id = access_key
        self.aws_secret_access_key = secret_key
        self.client = self._get_client()
        self.resource = self._get_resource()

    def _get_client(self):
        return boto3.client('ec2',aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key,region_name="us-west-2")

    def _get_resource(self):
        return boto3.resource('ec2',aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key,region_name="us-west-2")

    def deregister_image_name(self,name):
        response = self.client.describe_images(
                                        Filters=[
                                        {
                                            'Name': 'name',
                                            'Values': [name]
                                        }])
        if len(response["Images"]) == 1:
            image_id = response["Images"][0]["ImageId"]
            image = self.resource.Image(image_id)
            image.deregister()
            print("deregistered "+name)
    
    def create_ami(self, instance_id, name, description):
        AMI_TIMEOUT = 60*30
        AWS_API_WAIT_TIME = 1
        image_id = self.client.create_image(InstanceId=instance_id,Name=name,Description=description,NoReboot=True)["ImageId"]
        print("Checking if image is ready.")
        for _ in range(AMI_TIMEOUT):
            try:
                img = self.resource.Image(image_id)
                if img.state == 'available':
                    break
                else:
                    sleep(1)
            except ClientError as e:
                if e.response["Error"]["Code"]== 'InvalidAMIID.NotFound':
                    sleep(1)
                else:
                    raise Exception("Unexpected error code: {}".format(
                       e.response["Error"]["Code"]))
                sleep(1)
        else:
            raise Exception("Timeout waiting for AMI to finish")
        return image_id

    def ids_up(self,ids):
        NextToken = None
        up = []
        down = []
        ids_processed = 0
        while(len(ids)-ids_processed>200):
            up_chunk,down_chunk=self.ids_up(ids[ids_processed:ids_processed+200])
            up += up_chunk
            down += down_chunk
            ids_processed+=200
        while True and ids_processed<len(ids):
            if NextToken is not None:
                instances = self.client.describe_instances(
                    Filters=[
                            #{'Name': 'instance-state-name', 'Values': ['running']},
                            {'Name': 'instance-id', 'Values': ids[ids_processed:]}
                            ],
                    NextToken=NextToken)
            else:
                instances = self.client.describe_instances(
                    Filters=[
                            #{'Name': 'instance-state-name', 'Values': ['running']},
                            {'Name': 'instance-id', 'Values': ids[ids_processed:]}
                            ])
            try:
                NextToken = instances["NextToken"]
            except KeyError:
                NextToken = None
            for reservation in instances["Reservations"]:
                for instance in reservation["Instances"]:
                    if instance["State"]["Name"] == "running":
                        up.append(instance)
                    else:
                        down.append(instance)
            if NextToken is None: break
        if len(up)+len(down) != len(ids):
            print("Error: up and down should sum to ids")
            print("up: "+str(len(up)))
            print("down: "+str(len(down)))
            print("ids: "+str(len(ids)))
            quit()
        return up, down

    def start_100_or_less(self,imageid,count,security_groups,instance_type,key_name,userdata):
            if count > 100:
                print("ERROR: called start_100_or_less() with with count = "+count)
                quit()
            ids = []
            try:
                response = self.client.run_instances(ImageId=imageid,KeyName=key_name,InstanceType=instance_type,UserData=userdata,SecurityGroups=security_groups,MinCount=count,MaxCount=count)
            except ClientError as e:
                if e.response["Error"]['Code']=="InstanceLimitExceeded":
                    print("\nERROR: you can't start enough instances, make sure you are allowed to start "+str(count)+" "+instance_type+"(s) on AWS")
                    if len(ids) > 0:
                        self.terminate_slaves()
                    quit()
                else:
                    print("unexpected client error:")
                    print(e)
                    quit()
            for instance in response["Instances"]:
                ids.append(instance["InstanceId"])
            return ids

    def wait_is_running(self,ids):
        up,down = self.ids_up(ids)
        up_count = len(up)
        loop_timer = 0
        loop_period = 5
        sys.stdout.write("\r"+str(up_count)+"/"+str(len(ids))+" are running,"+str(len(down))+" are down, timeout in: "+str(3*60 - loop_timer)+"  ")
        while(up_count<len(ids)):
            sleep(loop_period)
            loop_timer += loop_period
            up,down = self.ids_up(ids)
            up_count = len(up)
            sys.stdout.write("\r"+str(up_count)+"/"+str(len(ids))+" are running,"+str(len(down))+" are down, timeout in: "+str(3*60 - loop_timer)+"  ")
            if loop_timer > 3*60 :
                print("\nERROR: some of the instances didn't come up after 3 minutes")
        print("\r"+str(up_count)+"/"+str(len(ids))+" are running,"+str(len(down))+" are down, timeout in: "+str(3*60 - loop_timer)+"  ")
        if up_count != len(ids):
            print(str(len(down))+" instances did not come to the running state")
        return up,down

    def wait_for_slave_count(self,count,timeout=5*60):
        while True:
            slaves = self.get_slaves()
            if len(slaves) >= count:
                break
            else:
                if timeout <= 0:
                    print("ERROR: not all slaves started")
                    quit()
                sleep(10)
                timeout-=10

    def wait_has_ip(self,ids):
        up_count = 0
        loop_timer = 0
        loop_period = 1
        down_count = 0
        ids_with_ip = []
        print("waiting for "+str(len(ids))+" ids to have ips")
        sys.stdout.write("\r"+str(up_count)+"/"+str(len(ids)))
        while(up_count<len(ids)):
            sleep(loop_period)
            loop_timer += loop_period
            up,down = self.ids_up(ids)
            down_count = len(down)
            up_count = 0
            ids_with_ip = []#TODO: improve rechecking logic
            for instance in up:
                try:
                    temp = instance["PublicIpAddress"]
                    if temp is not None:
                        up_count += 1
                        ids_with_ip.append(instance)
                except KeyError:
                    pass
            sys.stdout.write("\r"+str(up_count)+"/"+str(len(ids)))
        print("\r"+str(up_count)+"/"+str(len(ids)))
        ids_with_ip = [x["InstanceId"] for x in ids_with_ip]
        return ids_with_ip

    def wait_ssh_up_on_ips(self,ips):
        print("waiting for ssh to start on all instances.")
        #sub optimal loop: potentially requeries hosts already found to be up
        timeout = 3*60
        for ip in ips:
            if ip is None:
                print("ERROR: got a null ip")
                quit()
        not_up = list(ips)
        ssh_up_count = 0
        #TODO: concurrent query
        while(len(not_up)>0):
            sys.stdout.write("\rssh up: "+str(len(ips)-len(not_up))+"/"+str(len(ips))+"  ")
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.settimeout(2)#TODO: increase logrithmically
            rand = randint(0,len(not_up)-1)
            result = sock.connect_ex((not_up[rand],22))
            if result == 0:
                not_up.pop(rand)
        print("\rssh up: "+str(len(ips)-len(not_up))+"/"+str(len(ips))+"  ")
       
    def start_instances(self,imageid,count,security_groups,instance_type,key_name,userdata):
        remaining = count
        to_provision = 0
        ids = []
        print("provisioning instances")
        while(remaining > 0):
            to_provision = remaining
            if to_provision > 100:
                to_provision = 100
            sys.stdout.write("\rwaiting: "+str(remaining-to_provision)+", in progress: "+str(to_provision)+", done: "+str(len(ids))+"/"+str(count)+"  ")
            ids_returned = self.start_100_or_less(imageid,to_provision,security_groups,instance_type,key_name,userdata)
            ids += ids_returned
            #print("")
            #self.wait_for_slave_count(len(ids))
            remaining -= to_provision
        print("\rwaiting: "+str(remaining)+", in progress: 0, done: "+str(len(ids))+"/"+str(count)+"  ")
        ids_with_ip = self.wait_has_ip(ids)
        if len(ids_with_ip) != count:
            print("\nERROR: only got "+str(len(ids_with_ip))+" ids with IP when we asked for "+str(count))
            quit()
        print("\nstarted "+str(len(ids))+" "+instance_type+" instances")
        ips_map = self.get_ips(ids_with_ip)
        ips = [ips_map[key] for key in ips_map]
        self.wait_ssh_up_on_ips(ips)
        return ids

    def get_ips(self,ids):
        ips = {}
        ids_processed = 0
        while(len(ids)-ids_processed>200):
            ip_chunk = self.get_ips(ids[ids_processed:ids_processed+200])
            for key in ip_chunk:
                ips[key] = ip_chunk[key]
            ids_processed+=200
        else:
            NextToken = None
            while True and ids_processed<len(ids):
                if NextToken is not None:
                    response = self.client.describe_instances(InstanceIds=ids[ids_processed:],NextToken=NextToken)
                else:
                    response = self.client.describe_instances(InstanceIds=ids[ids_processed:])
                try:
                    NextToken = response["NextToken"]
                except KeyError:
                    NextToken = None
                for reservation in response["Reservations"]:
                    for instance in reservation["Instances"]:
                        if instance["InstanceId"] not in ids[ids_processed:]:
                            print("ERROR: did not find instance id <"+instance["InstanceId"]+"> in our input, but got it in the response from describe_instances")
                            quit()
                        try:
                            ips[instance["InstanceId"]] = instance["PublicIpAddress"]
                        except:
                            ips[instance["InstanceId"]] = None
                            print("did not find an ip for "+instance["InstanceId"])
                if NextToken is None: break
        if len(ips) != len(ids):
            print("ips:"+str(len(ips))+", ids:"+str(len(ids)))
            print("ERROR: failed to map all ids to ips")
            quit()
        return ips

    def get_slaves(self):
        NextToken = None
        slaves = []
        page_size = 1000
        #print("getting all slaves")
        while True:
            if NextToken is not None:
                instances = self.client.describe_instances(
                    Filters=[
                            {'Name': 'key-name', 'Values': ["slave_key"]}
                            ],
                    MaxResults=page_size,
                    NextToken=NextToken)
            else:
                instances = self.client.describe_instances(
                    Filters=[
                            {'Name': 'key-name', 'Values': ["slave_key"]}
                            ],
                    MaxResults=page_size)
            try:
                NextToken = instances["NextToken"]
                #print("next token: "+NextToken)
            except KeyError:
                #print("no next token")
                NextToken = None
            for reservation in instances["Reservations"]:
                instances = reservation["Instances"]
                slaves += instances
                #sys.stdout.write("\rfound "+str(len(slaves))+" slaves  ")
            if NextToken is None: break
        #print("\rfound "+str(len(slaves))+" slaves")
        slaves_not_term = []
        for slave in slaves:
            if slave["State"]["Name"] != "terminated" and slave["State"]["Name"] != "shutting-down":
                slaves_not_term.append(slave)
        print("slave count: "+str(len(slaves_not_term)))
        return slaves_not_term

    def terminate_slaves(self,depth=0):
        if depth > 100:
            print("ERROR: could not terminate all slaves")
            return
        #ec2.instances.filter(InstanceIds=ids).terminate()
        print("terminating instances")
        slaves = self.get_slaves()
        sys.stdout.write("\rremaining: "+str(len(slaves))+"   ")
        if len(slaves) == 0:
            print("\nno slaves")
            return
        while len(slaves) > 0:
            id_chunk = []
            for i in range(0,100):
                if len(slaves) > 0:
                    id_chunk.append(slaves.pop()["InstanceId"])
            instances = self.resource.instances.filter(InstanceIds=id_chunk)
            instances.terminate()
            sys.stdout.write("\rremaining: "+str(len(slaves))+"   ")
        print("")
        sleep(1)
        slaves = self.get_slaves()
        if len(slaves) > 0:
            print(str(len(slaves))+" slaves are still up, retrying")
            self.terminate_slaves(depth=depth+1)
        if len(slaves) == 0:
            print("all slaves terminated or shutting-down")

    def terminate_id(self,id_):
        instance = self.resource.instances.filter(InstanceIds=[id_])
        instance.terminate()

    def new_key(self,key_name,filepath):
        response = self.client.create_key_pair(KeyName=key_name)
        with open(filepath,"w") as f:
            f.write(response["KeyMaterial"])
        os.chmod(filepath,0o400)

    def delete_key(self,key_name,filepath):
        response = self.client.delete_key_pair(KeyName=key_name)
        try:
            os.remove(filepath)
        except FileNotFoundError as e:
            print("deleted key from AWS, but did not see a local private key to delete. proceeding.")
