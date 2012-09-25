__author__ = 'fmaia'
import logging
import paramiko

class Actuator(object):

    def __init__(self):

        logging.info('Actuator started.')

    def configureServer(self,server,servertag,available_machines=None):

        global TEMPLATE,TARGET,WHERETO

        #SERVER CONFIGURATION
        if servertag=='r':
            configFile(TEMPLATE,TARGET,0.55,0.1,0.07)
            copyToServer(server,WHERETO,TARGET)
        elif servertag=='w':
            configFile(TEMPLATE,TARGET,0.10,0.55,0.5)
            copyToServer(server,WHERETO,TARGET)
        elif servertag=='s':
            configFile(TEMPLATE,TARGET,0.55,0.1,0.07)
            copyToServer(server,WHERETO,TARGET)
        elif servertag=='rw':
            configFile(TEMPLATE,TARGET,0.45,0.20,0.15)
            copyToServer(server,WHERETO,TARGET)


            #Distribute (move) regions to regionservers and create the dynamic profile according to the distribution
    def distributeRegionsPerRS(self,regionStats=None,machines_to_regions=None):

        #MOVING REGIONS INTO PLACE
        for rserver in machines_to_regions:
            for region in machines_to_regions[rserver]:
                if not region.startswith('-ROOT') and not region.startswith('.META') and not region.startswith('load') and not region.startswith('len'):
                    ser = SERVER_LONG[rserver]
                    try:
                        metGlue.move(region,ser,False)
                    except Exception, err:
                        print 'ERROR:',err

                    if VERBOSE:
                        print 'Moving region ', region, ' to ', ser, ' DONE.'
                    #time.sleep(3) #- check if we need this

        while(isBusy()):
            time.sleep(5)

        refreshStats(False)
#	    for rserver in machines_to_regions:
#		    print stats[rserver]
#		    locality = stats[rserver]['hbase.regionserver.hdfsBlocksLocalityIndex']
#		    if (locality < '70' and MACHINE_TYPE[rserver]=="w") or (locality < '90' and MACHINE_TYPE[rserver]!="w"):
#			    for region in machines_to_regions[rserver]:
#				    if not region.startswith('-ROOT') and not region.startswith('.META') and not region.startswith('load') and not region.startswith('len'):
#					    try:
#						    print 'Major_Compact'
#						    metGlue.majorCompact(region)
#						    time.sleep(2)
#					    except Exception, err:
#						    print 'ERROR:',err
#

    #ADD MACHINE
    def tiramolaAddMachine(self):

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        #images = eucacluster.describe_images(self.utils.bucket_name)
        #print "Found emi in db: " + str(images[0].id)
        instances = eucacluster.describe_instances()
        maxID=0
        for instance in instances:
            if (instance.name.startswith("region")):
                num=int(instance.name[6:])
                if num > maxID:
                    maxID=num
        print maxID
        name="region"+str(maxID+1)
        instances = eucacluster.run_instances("8a68885f-99eb-4858-808d-7d518f3bb837", name, None, None, 1, 1, None)
        print "Launched new instance: " + str(instances)
        mInstances = eucacluster.block_until_running(instances)
        for instance in mInstances:
            hosts = open('/tmp/hosts', 'a')
            try:
                ssh.connect(instance.public_dns_name, username='root', password='123456')
            except:
                print("Unable to connect to node  " + instance.public_dns_name)

            #ADDED THIS TO FIX GANGLIA PROBLEM
            stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor stop')
            print(str(stdout.readlines()))

            stdin, stdout, stderr = ssh.exec_command('echo \"'+instance.name+"\" > /etc/hostname")
            print(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('hostname \"'+instance.name+"\"")
            print(str(stdout.readlines()))
            hosts.write(instance.private_dns_name + "\t" + instance.name +"\n")
            print "New instance:",instance.name[6:]
            stdin, stdout, stderr = ssh.exec_command('reboot')
            mInstances = eucacluster.block_until_running([instance])
        hosts.close()

        for node in ["master","10.0.108.16","10.0.108.19", mInstances[0].public_dns_name]:
            transport = paramiko.Transport((node, 22))
            try:
                transport.connect(username = 'root', password = '123456')
            except:
                print("Unable to connect to node  " + node)
            transport.open_channel("session", node, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            print("Sending host to node:  " + node)
            sftp.put( "/tmp/hosts", "/etc/hosts")
            sftp.close()

        os.system("echo '123456'|sudo -S cp /tmp/hosts /etc/hosts")

        for instance in mInstances:
            try:
                ssh.connect(instance.public_dns_name, username='root', password='123456')
            except:
                print("Unable to connect to node  " + instance.public_dns_name)

            #stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
            #print(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/hadoop-daemon.sh start datanode')
            print(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0-cdh4b1-rmv/bin/hbase-daemon.sh start regionserver')
            print(str(stdout.readlines()))
            #time.sleep(60)

        #RESTART GANGLIA TO FIX THE PROBLEM OF OPENSTACK RUNNING THE DEAMON
        print "Restarting ganlgia on Master"
        tries=0
        while tries<10:
            try:
                tries+=1
                ssh.connect("master", username='root', password='123456')
                break
            except:
                print("Unable to connect to node  " + "master"+ " after "+str(tries)+" attempts.")
        stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
        print(str(stdout.readlines()))
        ssh.close()

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for clusterkey in clusterHBase:
            if not clusterkey.endswith("master"):
                print "Restarting ganlgia on Slave:",clusterkey
                tries=0
                while tries<10:
                    try:
                        tries+=1
                        ssh.connect(clusterkey, username='root', password='123456')
                        break
                    except:
                        print("Unable to connect to node  " + clusterkey+ " after "+str(tries)+" attempts.")
                stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
                print(str(stdout.readlines()))
                ssh.close()
        for instance in mInstances:
            try:
                ssh.connect(instance.public_dns_name, username='root', password='123456')
                stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
                print(str(stdout.readlines()))
                ssh.close()
            except:
                print("Unable to connect to node  " + instance.public_dns_name)
