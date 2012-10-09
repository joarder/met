import copy

__author__ = 'fmaia'
import logging
import MeTGlue
import MonitorVms
import monitor_config
from threading import Thread

class Stats(object):


    def __init__(self):
        self._clusterHBase = []
        self._stats = {}
        self._metGlue =  MeTGlue.MeTGlue()
        logging.info('Connected to MeTGlue gateway.')
        self._rserver_longname = {}
        self._region_metrics = {}
        self._monVms = MonitorVms.MonitorVms(None)
        logging.info('Connected to Ganglia.')
        self._metric_filter = ["cpu_idle","cpu_wio","hbase.regionserver.hdfsBlocksLocalityIndex"]
        self._ALPHA = monitor_config.alpha
        self.refreshStats(False)
        logging.info('Stats started.')
        self._lock = Thread.allocate_lock()


    def getMeTGlue(self):
        return self._metGlue

    def getNumberRegionServers(self):
        return len(self._clusterHBase)

    def getRegionServers(self):
        return copy.deepcopy(self._clusterHBase)

    def getRegionServerStats(self,rserver):
        return copy.deepcopy(self._stats[rserver])

    def getServerLongNames(self):
        return self._rserver_longname

    def getServerLongName(self,server):
        return self._rserver_longname[server]

    def getRegionStats(self):
        return copy.deepcopy(self._region_metrics)

    def resetStats(self):
        self._clusterHBase = []
        self._stats = {}
        self._rserver_longname = {}
        self._region_metrics = {}


    def refreshStats(self,CYCLE=True):

        self._lock.acquire()
        self._clusterHBase = []
        #get new stats
        ganglia_metrics = self._monVms.refreshMetrics()
        cluster_metrics = self._metGlue.getRegionServerStats(False)
        for serverid in cluster_metrics.keys():
            short = str(serverid).split(',')[0]
            self._clusterHBase.append(short)
            self._rserver_longname[short] = serverid

        self._region_metrics = self._metGlue.getRegionStats(False)
        regionservers = ganglia_metrics.keys()

        #combined stats to process - using alpha smoothing technique
        for key in regionservers:
            if key in self._clusterHBase and key in regionservers:
                if key not in self._stats.keys():
                    self._stats[key] = {}
                for kmetric in ganglia_metrics[key].keys():
                    if kmetric in self._stats[key].keys() and kmetric in self._metric_filter:
                        value_ = ganglia_metrics[key][kmetric]
                        old_value = self._stats[key][kmetric]
                        self._stats[key][kmetric] = (self._ALPHA*float(value_)) + ((1-self._ALPHA)*float(old_value))
                    elif kmetric in self._metric_filter:
                        self._stats[key][kmetric] = ganglia_metrics[key][kmetric]

        for k in cluster_metrics.keys():
            key = str(k).split(',')[0]
            tmp_stats = cluster_metrics[k]
            for subkey in tmp_stats.keys():
                self._stats[key][subkey] = tmp_stats[subkey]

        if CYCLE:
            for rserver in self._stats.keys():
                logging.info(rserver+' cpu_idle:'+str(self._stats[rserver]['cpu_idle'])+" cpu_wio:"+str(self._stats[rserver]['cpu_wio'])+" locality:"+str(self._stats[rserver]['hbase.regionserver.hdfsBlocksLocalityIndex']))
        else:
            logging.info('Stats refreshed. Servers: '+str(self._clusterHBase))

        self._lock.release()
