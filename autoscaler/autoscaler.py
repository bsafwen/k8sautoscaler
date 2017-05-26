'''
Created on May 26, 2017

@author: bsafwene
'''
from kubernetes import client, config
import requests
import time

class Autoscaler(object):
    def __init__(self,**kwargs):
        """
        :param str heapsterIP: the IP address of heapster server
        :param str heapsterPort: the port heapster is listening on
        :param str configFile: a path to the k8s config file
        :param str namespace: a string denoting the namespace
        :param float period: period to sleep between each run
        """
        config.load_kube_config(kwargs['configFile'])
        self.v1beta1 = client.apis.AppsV1beta1Api()  # @UndefinedVariable
        self.v1 = client.CoreV1Api()
        self.namespace = kwargs['namespace'] 
        self.heapsterIP = kwargs['heapsterIP']
        self.heapsterPort = kwargs['heapsterPort']
        self.period = kwargs['period']
        
    def getPod(self, pod):
        """
        :param: str pod: the pod name
        :return: returns an kubernetes.client.V1Pod object with name pod
        :rtype: kubernetes.client.V1Pod
        """
        return self.v1.read_namespaced_pod(name=pod, namespace=self.namespace)
    
    def getListStatefulsets(self):
        """
        :return: a list of kubernetes.client.V1beta1StatefulSet objects
        :rtype: list
        """
        resp = self.v1beta1.list_namespaced_stateful_set(namespace=self.namespace)
        return resp.items()
    
    def getPodCpu(self, pod):
        """
        :param: str pod: pod name
        :return: cpu usage of the pod
        :rtype: float
        """
        return  int(requests.get("http://"+self.heapsterIP+":"+
                         self.heapsterPort+
                         "/api/v1/model/namespaces/"+
                         self.namespace+"/pods/"+
                         pod+
                         "/metrics/cpu/usage_rate").json()
                         ['metrics'][-1]['value'])
        
    def getPodCpuPercentage(self, pod):
        """
        :param: str pod: pod name
        :return: cpu_usage * 100 / cpu_limit
        :rtype: float
        """
        v1pod = self.getPod(pod)
        cpu = self.getPodCpu(v1pod.metadata().name())
        containers = v1pod.spec().containers()
        cpulimit = containers[0].resources().limits['cpu']
        if cpulimit == None :
            cpulimit = 1000
        return (cpu*100)/cpulimit
        
    def getStatefulsetAvgCpuPercentage(self, statefulset):
        """
        :param: V1beta1StatefulSet statefulset
        :return: Average cpu percentage of all the pods in the statefulset
        :rtype: float
        """
        replicas = statefulset.spec().replicas()
        podbasename = statefulset.metadata().name()
        avg = 0
        for i in range(replicas):
            avg = avg + self.getPodCpu(podbasename+"-"+str(i))
        return avg/replicas
    
    def scaleUp(self, statefulset):
        """
        :param: V1beta1StatefulSet statefulset
        """
        replicas = statefulset.spec().replicas()
        d={"spec": {"replicas":str(replicas)+1}}
        self.v1beta1.patch_namespaced_stateful_set(        body=d,
                                                           name=statefulset.metadata.name, 
                                                           namespace=self.namespace)

        
    def run(self):
        """
        Runs the autoscaler, the autoscaler will check the state of the statefulsets every self.period
        """
        while True:
            l = self.getListStatefulsets()
            for s in l:
                labels = s.metadata().labels()
                if labels['autoscaling'] == "True" :
                    _max = float(labels['autoscaling_max_cpu_percent'])
                    cpu = self.getStatefulsetAvgCpuPercentage(s)
                    if cpu > _max:
                        self.scaleUp(s)
            time.sleep(self.period)
            

a = Autoscaler("/home/bsafwene/pfe/admin.conf")

print(a)