'''
ck
Created on May 7, 2012

@author: rmpvilaca
'''


from novaclient.v1_1 import client
import sys, os, time
import Utils
import commands

class OpenStackCluster(object):
    '''
    This class holds all instances that take part in the virtual cluster.
    It can create and stop new instances - and working in conjuction with the
    db specific classes set up various environments. 
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.utils = Utils.Utils()


    def describe_instances(self, state=None, pattern=None):
        instances = []
        nt = client.Client('admin', 'nova', 'project', 'http://192.168.111.202:5000/v2.0',service_type='compute')
        reservations=nt.servers.list()

        members = ("id", "image_id", "public_dns_name", "private_dns_name",
                   "state", "key_name", "ami_launch_index", "product_codes",
                   "instance_type", "launch_time", "placement", "kernel",
                   "ramdisk","name")
        print "Servers:",reservations
        reservations=filter(lambda x:x.status=="ACTIVE",reservations)
        print "Reservations:",reservations
        for instance in reservations:
            details = {}
            for member in members:
                if (member == "image_id"):
                    val=instance.image['id']
                elif (member == "public_dns_name"):
                    val=instance.addresses['project_network'][0]['addr']
                elif (member == "private_dns_name"):
                    val=instance.addresses['project_network'][0]['addr']
                elif (member == "state"):
                    val=instance.status
                else:
                    val = getattr(instance, member, "")
                if val is None: val = ""
                # product_codes is a list
                if hasattr(val, '__iter__'):
                    val = ','.join(val)
                details[member] = val
            for var in details.keys():
                exec "instance.%s=\"%s\"" % (var, details[var])
            if state:
                if state == instance.state:
                    instances.append(instance)
            else:
                instances.append(instance)

        print "State:",state,";Pattern:",pattern,str(instances)
        ## if you are using patterns and state, show only matching state and id's
        matched_instances = []
        if pattern:
            for instance in instances:
                if instance.id.find(pattern) != -1:
                    matched_instances.append(instance)
            if len(matched_instances) > 0:
                return matched_instances
            else:
                return None
        else:
            return instances


    def describe_images(self, pattern=None):
	all_ids = False
	owners = [ ]
	executable_by = [ ]
	image_ids = [ ]

	try:
	    nt = client.Client('admin', 'nova', 'project', 'http://192.168.111.202:5000/v2.0',service_type='compute')
	except Exception, e:
	    print e.message
	    sys.exit(1)
	if all_ids and (len(owners) or len(executable_by) or len(image_ids)):
	    print "-a cannot be combined with owner, launch, or image list"

	if len(owners) == 0 and len(executable_by) == 0 and\
	   len(image_ids) == 0 and not all_ids:

	    try:
		images=nt.images.list()

		owned = nt.images.list()
		launchable = nt.images.list()
		mylist = [ ]
		images = [ ]
		for image in owned:
		    mylist.append(image.id)
		    images.append(image)
		for image in launchable:
		    if image.id not in mylist:
			images.append(image)
	    except Exception, ex:
		print ex
	else:
	    try:
		images=nt.images.list()
	    except Exception, ex:
		print ex
	print "Images:", images

	## if you are using patterns, show only matching names and emi's
	matched_images = []
	print "Pattern",pattern
	if pattern:
	    for image in images:
		if image.name.find(pattern) != -1:
		    matched_images.append(image)
	    print "Match images:",matched_images
	    if len(matched_images) > 0:
		return matched_images
	    else:
		return None
	else:
	    return images

    def read_user_data(self, user_data_filename):
	USER_DATA_CHUNK_SIZE = 512
	user_data = "";
	user_data_file = open(user_data_filename, "r")
	while 1:
	    data = user_data_file.read(USER_DATA_CHUNK_SIZE)
	    if not data:
		break
	    user_data += data
	user_data_file.close()
	return user_data

    def run_instances(self, image_id=None,
		      keyname=None,
		      kernel_id=None,
		      ramdisk_id=None,
		      min_count=1,
		      max_count=1,
		      instance_type='m1.medium',
		      group_names=[],
		      user_data=None,
		      user_data_file=None,
		      addressing_type="public",
		      zone=None):
	reservation = None


	if image_id:
	    if not user_data:
		if user_data_file:
		    user_data = self.read_user_data(user_data_file)
	    try:
		nt = client.Client('admin', 'nova', 'project', 'http://192.168.111.202:5000/v2.0',service_type='compute')
	    except Exception, e:
		print e.message
	    try:
		image = filter(lambda x:x.name=='met-nsdi-ganglia',nt.images.list())[0]
		flavor = filter(lambda x:x.id=='6',nt.flavors.list())[0]
		print image,flavor
		new_instance = nt.servers.create(image=image, flavor=flavor, name=keyname,
		    #min_count=min_count,max_count=max_count,
		    userdata=user_data,keyname=None,security_groups=['hbase'])
	    except Exception, ex:
		print ex.message
		sys.exit(3)
	else:
	    print 'image_id must be specified'
	print new_instance.id
	time.sleep(10)
	new_instance=filter(lambda x:x.id==new_instance.id,nt.servers.list())[0]
	instances = []
	print "Address:",new_instance.addresses['project_network']

	## add the newly run instances to the database
	members = ("id", "image_id", "public_dns_name", "private_dns_name",
		   "state", "key_name", "ami_launch_index", "product_codes",
		   "instance_type", "launch_time", "placement", "kernel",
		   "ramdisk","name")

	details = {}
	for member in members:
	    if (member == "image_id"):
		val=new_instance.image['id']
	    elif (member == "public_dns_name"):
		val=new_instance.addresses['project_network'][0]['addr']
	    elif (member == "private_dns_name"):
		val=new_instance.addresses['project_network'][0]['addr']
	    elif (member == "state"):
		val=new_instance.status
	    else:
		val = getattr(new_instance, member, "")
		# product_codes is a list
	    if val is None: val = ""
	    if hasattr(val, '__iter__'):
		val = ','.join(val)
	    details[member] = val
	print "Details:"+ str(details)
	for var in details.keys():
	    exec "new_instance.%s=\"%s\"" % (var, details[var]) in locals()
	print "New Instance:",new_instance
	instances.append(new_instance)
	return instances

    def terminate_instances(self, instance_ids):
	if (len(instance_ids) > 0):
	    try:
		nt = client.Client('admin', 'nova', 'project', 'http://192.168.111.202:5000/v2.0',service_type='compute')
	    except Exception, e:
		print e.message
	    try:
		for id in instance_ids:
		    nt.servers.delete(id)
	    except Exception, ex:
		print e.message

	else:
	    print 'instance id(s) must be specified.'


    #
    ## Utilities
    #
    def block_until_running (self, instances):
	''' Blocks until all defined instances have reached running state and an ip has been assigned'''
	## Run describe instances until everyone is running
	tmpinstances = instances
	instances = []
	while len(tmpinstances) > 0 :
	    time.sleep(10)
	    print "Waiting for", len(tmpinstances), "instances."
	    sys.stdout.flush()
	    all_running_instances = self.describe_instances("ACTIVE")
	    #print "ALL",str(all_running_instances)
	    #print "TMP",str(tmpinstances)
	    for i in range(0,len(all_running_instances)):
		for j in range(0,len(tmpinstances)):
		    ping = commands.getoutput("/bin/ping -q -c 1 " + str(all_running_instances[i].public_dns_name))
		    nc = commands.getoutput("nc -z  -v "+ str(all_running_instances[i].public_dns_name)+" 22")
		    #print "IP:",str(all_running_instances[i].public_dns_name),";ping:",ping,";nc:",nc
		    #print all_running_instances[i].id, all_running_instances[j].id
		    if (all_running_instances[i].id == tmpinstances[j].id)\
		       and ping.count('1 received') > 0 and nc.count("succeeded") > 0:
			tmpinstances.pop(j)
			print "Append:",all_running_instances[i]
			instances.append(all_running_instances[i])
			break
	self.describe_instances()
	#print "Instance BLOCK:",instances
	return instances


if __name__ == "__main__":
    euca = OpenStackCluster()
    euca.describe_instances("ACTIVE")

