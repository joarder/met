__author__ = 'fmaia'
import logging
import actuator_config
import os
import paramiko
import time
import MeTGlue
import OpenStackCluster

class Actuator(object):

    def __init__(self,stats):
        self._metglue = stats.getMeTGlue()
        self._stats = stats
        self._eucacluster = OpenStackCluster.OpenStackCluster()
        #Actuator Parameters
        self._TEMPLATE = actuator_config.template
        self._TARGET = actuator_config.target
        self._WHERETO = actuator_config.whereto
        self._USERNAME = actuator_config.username
        self._PASSWORD = actuator_config.password
        self._MASTER = actuator_config.master
        logging.info('Actuator started.')

    def copyToServer(self,host,whereto,filepath):
        logging.info("Copying files to ", host)
        transport = paramiko.Transport((host, 22))
        tries=0
        while tries<100:
            try:
                tries+=1
                transport.connect(username = self._USERNAME, password = self._PASSWORD)
                break
            except:
                print ("Unable to connect to node  " + host+ " after "+str(tries)+" attempts.")
                time.sleep(5)

        transport.open_channel("session", host, "localhost")
        sftp = paramiko.SFTPClient.from_transport(transport)
        splittedpath = filepath.split('/')[-1]
        sftp.put(filepath, whereto+'/'+splittedpath)
        sftp.close()
        logging.info('File ',filepath,' copied to ',host,'.')


    def configFile(self,template,final,block,memu,meml):
        os.system("sed 's/BLOCKCACHESIZE/"+str(block)+"/g; s/GLOBALMEMSTOTEUPPERLIMIT/"+str(memu)+"/g; s/GLOBALMEMSTORELOWERLIMT/"+str(meml)+"/g' " + template + " > " + final)
        print 'File ',template,' configured with block:',str(block),' memu:',str(memu),' meml:',str(meml)


    def configureServer(self,server,servertag):
        #SERVER CONFIGURATION
        if servertag=='r':
            self.configFile(self._TEMPLATE,self._TARGET,0.55,0.1,0.07)
            self.copyToServer(server,self._WHERETO,self._TARGET)
        elif servertag=='w':
            self.configFile(self._TEMPLATE,self._TARGET,0.10,0.55,0.5)
            self.copyToServer(server,self._WHERETO,self._TARGET)
        elif servertag=='s':
            self.configFile(self._TEMPLATE,self._TARGET,0.55,0.1,0.07)
            self.copyToServer(server,self._WHERETO,self._TARGET)
        elif servertag=='rw':
            self.configFile(self._TEMPLATE,self._TARGET,0.45,0.20,0.15)
            self.copyToServer(server,self._WHERETO,self._TARGET)


    def isBusy(self):
        x = os.popen("curl \"http://"+self._MASTER+":60010/master-status\"").read()
        return (not "No regions in transition." in x)

    #Distribute (move) regions to regionservers
    def distributeRegionsPerRS(self,machines_to_regions=None,machine_type=None):
        longServerNames = self._stats.getServerLongNames()
        #MOVING REGIONS INTO PLACE
        for rserver in machines_to_regions:
            for region in machines_to_regions[rserver]:
                if not region.startswith('-ROOT') and not region.startswith('.META') and not region.startswith('load') and not region.startswith('len'):
                    ser = longServerNames[rserver]
                    try:
                        self._metglue.move(region,ser,False)
                    except Exception, err:
                        logging.error('ERROR:',err)
                    logging.info('Moving region ', region, ' to ', ser, ' DONE.')

        while(self.isBusy()):
            time.sleep(5)

        self._stats.refreshStats(False)
        for rserver in machines_to_regions:
            rserver_stats = self._stats.getRegionServerStats(rserver)
            locality = rserver_stats['hbase.regionserver.hdfsBlocksLocalityIndex']
            if (locality < '70' and machine_type[rserver]=="w") or (locality < '90' and machine_type[rserver]!="w"):
			    for region in machines_to_regions[rserver]:
				    if not region.startswith('-ROOT') and not region.startswith('.META') and not region.startswith('load') and not region.startswith('len'):
					    try:
						    logging.info('Major compact of: ',region)
						    self._metglue.majorCompact(region)
						    time.sleep(2)
					    except Exception, err:
						    logging.error('ERROR:',err)


    #ADD MACHINE
    def tiramolaAddMachine(self):

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        instances = self._eucacluster.describe_instances()
        maxID=0
        for instance in instances:
            if (instance.name.startswith("region")):
                num=int(instance.name[6:])
                if num > maxID:
                    maxID=num
        name="region"+str(maxID+1)
        instances = self._eucacluster.run_instances(" ", name, None, None, 1, 1, None)
        logging.info("Launched new instance: " + str(instances))
        mInstances = self._eucacluster.block_until_running(instances)
        for instance in mInstances:
            hosts = open('/tmp/hosts', 'a')
            try:
                ssh.connect(instance.public_dns_name, username=self._USERNAME, password=self._PASSWORD)
            except:
                logging.error("Unable to connect to node  " + instance.public_dns_name)

            #ADDED THIS TO FIX GANGLIA PROBLEM
            stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor stop')
            logging.info(str(stdout.readlines()))

            stdin, stdout, stderr = ssh.exec_command('echo \"'+instance.name+"\" > /etc/hostname")
            logging.info(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('hostname \"'+instance.name+"\"")
            logging.info(str(stdout.readlines()))
            hosts.write(instance.private_dns_name + "\t" + instance.name +"\n")
            stdin, stdout, stderr = ssh.exec_command('reboot')
            mInstances = self._eucacluster.block_until_running([instance])

        hosts.close()

        for node in ["master","10.0.108.16","10.0.108.19", mInstances[0].public_dns_name]:
            transport = paramiko.Transport((node, 22))
            try:
                transport.connect(username = 'root', password = '123456')
            except:
                logging.error("Unable to connect to node  " + node)
            transport.open_channel("session", node, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info("Sending /etc/hosts to node:  " + node)
            sftp.put( "/tmp/hosts", "/etc/hosts")
            sftp.close()

        os.system("echo '"+self._PASSWORD+"' |sudo -S cp /tmp/hosts /etc/hosts")

        for instance in mInstances:
            try:
                ssh.connect(instance.public_dns_name, username='root', password='123456')
            except:
                logging.error("Unable to connect to node  " + instance.public_dns_name)

            stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/hadoop-daemon.sh start datanode')
            logging.info(str(stdout.readlines()))
            stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0-cdh4b1-rmv/bin/hbase-daemon.sh start regionserver')
            logging.info(str(stdout.readlines()))

        #RESTART GANGLIA TO FIX THE PROBLEM OF OPENSTACK RUNNING THE DEAMON
        logging.info("Restarting ganlgia on Master.")
        tries=0
        while tries<10:
            try:
                tries+=1
                ssh.connect("master", username='root', password='123456')
                break
            except:
                logging.error("Unable to connect to node  " + "master"+ " after "+str(tries)+" attempts.")
        stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
        logging.info(str(stdout.readlines()))
        ssh.close()

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for clusterkey in self._stats.getRegionServers():
            if not clusterkey.endswith("master"):
                logging.info("Restarting ganlgia on Slave:",clusterkey)
                tries=0
                while tries<10:
                    try:
                        tries+=1
                        ssh.connect(clusterkey, username=self._USERNAME, password=self._PASSWORD)
                        break
                    except:
                        logging.error("Unable to connect to node  " + clusterkey+ " after "+str(tries)+" attempts.")
                stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
                logging.info(str(stdout.readlines()))
                ssh.close()
        for instance in mInstances:
            try:
                ssh.connect(instance.public_dns_name, username=self._USERNAME, password=self._PASSWORD)
                stdin, stdout, stderr = ssh.exec_command('/etc/init.d/ganglia-monitor restart')
                logging.info(str(stdout.readlines()))
                ssh.close()
            except:
                logging.error("Unable to connect to node  " + instance.public_dns_name)
