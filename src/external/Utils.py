import os
from ConfigParser import ConfigParser

class Utils(object):
    '''
    This class holds utility functions. 
    '''
    
    def __init__(self):
        self.read_properties(os.getenv("HOME", "/etc") + "/Coordinator.properties")
	 ## Install stdout
        self.stdout = self.install_dir+"/logs/Coordinator.out"


    def read_properties(self, property_file="Coordinator.properties"):
            """ process properties file """
            ## Reads the configuration properties
            cfg = ConfigParser()
            cfg.read(property_file)
            self.install_dir = cfg.get("config", "install_dir")
            self.euca_rc_dir = cfg.get("config", "euca_rc_dir")
            self.initial_cluster_size = cfg.get("config", "initial_cluster_size")
            self.max_cluster_size = cfg.get("config", "max_cluster_size")
            self.bucket_name = cfg.get("config", "bucket_name")
            self.instance_type = cfg.get("config", "instance_type")
            self.cluster_name = cfg.get("config", "cluster_name")
            self.hostname_template = cfg.get("config", "hostname_template")
            self.reconfigure = cfg.get("config", "reconfigure")
            self.cluster_type = cfg.get("config", "cluster_type")
            self.db_file = cfg.get("config", "db_file")
            self.add_nodes = cfg.get("config", "add_nodes")
            self.rem_nodes = cfg.get("config", "rem_nodes")
            self.cloud_api_type = cfg.get("config", "cloud_api_type")
            self.trans_cost = cfg.get("config", "trans_cost")
            self.gain = cfg.get("config", "gain")
            self.serv_throughput = cfg.get("config", "serv_throughput")
            try:
                self.gamma = cfg.get("config", "gamma")
            except:
                self.gamma = 0
            
            ## Reads the monitoring thresholds
            self.thresholds_add = {}
            self.thresholds_remove = {}
            for option in cfg.options("thresholds_add"):
                self.thresholds_add[option] = cfg.get("thresholds_add", option)
            for option in cfg.options("thresholds_remove"):
                self.thresholds_remove[option] = cfg.get("thresholds_remove", option)
            
            
