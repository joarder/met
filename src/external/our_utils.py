#from xml.dom.minidom import parse
import sys, os, paramiko

def stopServer(host):
	ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(hostname=host, username='gsd', key_filename='/Users/fmaia/.ssh/gsd_private_key',password='123456')
	stdin, stdout, stderr = ssh.exec_command("echo '123456' | sudo -S -u hdfs /usr/lib/hbase/bin/hbase-daemon.sh stop regionserver > /dev/null")
	print stdin
	print stdout
	print stderr


def restartServer(host):
	ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(hostname=host, username='gsd', key_filename='/Users/fmaia/.ssh/gsd_private_key',password='123456')
	#stdin, stdout, stderr = ssh.exec_command("echo '123456' > ~/coco.txt")
	stdin, stdout, stderr = ssh.exec_command("echo '123456' | sudo -S -u hdfs /usr/lib/hbase/bin/hbase-daemon.sh restart regionserver > /dev/null")
	print stdin
	print stdout
	print stderr

def copyToServer(host,whereto,filepath):
	os.system("scp "+filepath+" "+host+":"+whereto)

def configFile(template,final,block,memu,meml):
	os.system("sed 's/BLOCKCACHESIZE/"+block+"/g; s/GLOBALMEMSTOTEUPPERLIMIT/"+memu+"/g; s/GLOBALMEMSTORELOWERLIMT/"+meml+"/g' " + template + " > " + final)

if __name__ == '__main__':

	TEMPLATE = "/Users/fmaia/Workspace/MeT/MeTGlue/python/tmp/hbase-site_template.xml "
	TARGET = "/Users/fmaia/Workspace/MeT/MeTGlue/python/tmp/hbase-site.xml"
	restartServer('192.168.111.226')
	#copyToServer('192.168.111.221',' ',"/Users/fmaia/Workspace/MeT/MeTGlue/python/tmp/hbase-site_template.xml")
	#stopServer('192.168.111.223')
