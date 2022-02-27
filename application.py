# -*- coding  :   utf-8 -*-
# @Author     :   zhaojiangbing
# @File       :   application.py
# @Software   :   PyCharm


"""该模块封装eureka的app和instance。"""


import json
import socket
import traceback

from log import manage_log
from .load_balance import LoadBalance


logger = manage_log.get_logger(__name__)


class App(object):
    """实现具体的应用(server)。"""

    def __init__(self, author, name):
        """

        :param author: EurekaClient实例。
        :param name: str，app名字。
        """

        self._author = author
        self._name = name
        self._instances = {}

        self.load_balance = LoadBalance(self)  # 实例化负载均衡对象

    def create_instance(self, instance_id=None, metadata={}, **kwargs):
        """创建一个服务实例。

            :param app: App对象。
            :param hostname: str，被注册服务实例的主机名。
            :param ip_addr: str，被注册服务实例的ip。
            :param port: int，被注册服务实例的端口。
            :param instance_id: int，被注册服务实例的id。
            :param metadata: dict，被注册服务实例的原信息。
            :param lease_duration: int，单位秒，超过这个间隔没发送心跳，就认为服务实例挂了。
            :param lease_renewal_interval: int，单位秒，服务实例续租的间隔。
            :param health_check_url: str，Health check URL if available, not required.
                                     But if included it should return 2xx。
            :param status_page_url: str,URL for server status (info route?), It's
                                    required to not crash the Spring Eureka UI,
                                    but otherwise not required. If not included -
                                    we will just use the server IP with '/info'。
            """

        if "weight" not in metadata:
            metadata["weight"] = 1  # 设置实例默认权重为1

        if instance_id in self._instances:
            self.update_instance(instance_id=instance_id, metadata=metadata, **kwargs)  # 更新
        else:
            instance = Instance(app=self, instance_id=instance_id, metadata=metadata, **kwargs)  # 创建
            instance_id = instance.instance_id
            self.add_instance(instance)
        return self._instances[instance_id]

    def update_instance(self, instance_id, **kwargs):
        instance = self._instances[instance_id]
        instance._metadata = kwargs.get("metadata", instance._metadata)
        instance._lease_duration = kwargs.get("lease_duration", instance._lease_duration)
        instance._lease_renewal_interval = kwargs.get("lease_renewal_interval", instance._lease_renewal_interval)
        instance._ip_addr = kwargs.get("ip_addr", instance._ip_addr)
        instance._port = kwargs.get("port", instance._port)
        instance._hostname = kwargs.get("hostname", instance._hostname)
        instance._health_check_url = kwargs.get("health_check_url", instance._health_check_url)
        instance._status_page_url = kwargs.get("status_page_url", instance._status_page_url)

    async def get_instance(self, instance_id, is_remote=False):
        """获取实例(service)。

        :param instance_id: str，实例的id。
        :param is_remote: bool，是否从远端获取。
        :return: Instance对象 or None。。
        """

        if instance_id not in self._instances or is_remote == True:
            try:
                url = "/apps/{}/{}".format(self._name, instance_id)
                result = await self._author._do_req(url)
                instance = result["instance"]
                self.create_instance(hostname=instance["hostName"],
                                     ip_addr=instance["ipAddr"],
                                     port=int(instance["port"]["$"]),
                                     instance_id=instance["instanceId"],
                                     metadata=instance["metadata"],
                                     lease_duration = int(instance["leaseInfo"]["durationInSecs"]),
                                     lease_renewal_interval = int(instance["leaseInfo"]["renewalIntervalInSecs"]),
                                     # health_check_url = instance["health_check_url"],
                                     status_page_url = instance["statusPageUrl"]
                )
            except:
                logger.adebug(traceback.format_exc())
        return self._instances.get(instance_id, None)

    def add_instance(self, instance):
        """添加实例

        :param instance: Instance对象。
        :return:
        """

        self.remove_instance(instance)  # 如果instance存就删除，
        self._instances[instance.instance_id] = instance

    def remove_instance(self, instance):
        """删除实例

        :param instance: Instance对象
        :return:
        """

        if instance.instance_id in self._instances:
            del self._instances[instance.instance_id]

    def get_instance_ids(self):
        return sorted(self._instances.keys())

    @property
    def _str_(self):
        return "_name: {}".format(self._name)


class Instance(object):
    """应用的具体实例(service)类。"""

    def __init__(self, app, hostname=None, ip_addr=None, port=8080, instance_id=None, metadata={},
                 lease_duration=30, lease_renewal_interval=10,
                 health_check_url=None, status_page_url=None):
        """
        :param app: App对象。
        :param hostname: str，被注册服务实例的主机名。
        :param ip_addr: str，被注册服务实例的ip。
        :param port: int，被注册服务实例的端口。
        :param instance_id: int，被注册服务实例的id。
        :param metadata: dict，被注册服务实例的原信息。
        :param lease_duration: int，单位秒，超过这个间隔没发送心跳，就认为服务实例挂了。
        :param lease_renewal_interval: int，单位秒，服务实例续租的间隔。
        :param health_check_url: str，Health check URL if available, not required.
                                 But if included it should return 2xx。
        :param status_page_url: str,URL for server status (info route?), It's
                                required to not crash the Spring Eureka UI,
                                but otherwise not required. If not included -
                                we will just use the server IP with '/info'。
        """

        self._app = app
        self._is_register = False  # 如果注册了就为True
        self._is_heartbeat = True  # 如果为False，就停止心跳
        self._metadata = metadata
        self.dynamic_weight = 0  # 动态权重，用于负载均衡的时候用。
        self._lease_duration = lease_duration
        self._lease_renewal_interval = lease_renewal_interval
        self._ip_addr = ip_addr or self.get_local_ip()
        self._port = port
        self._hostname = hostname or self._ip_addr
        self._instance_id = instance_id
        self._health_check_url = health_check_url

        if "weight" not in self._metadata:
            self._metadata["weight"] = 1   # 设置默认权重为1

        if status_page_url is None:
            status_page_url = "http://{}:{}/info".format(self._ip_addr, port)
        self._status_page_url = status_page_url

    @property
    def instance_id(self):
        return self._instance_id or self._get_id()

    def _get_id(self):
        return "{}:{}".format(self._ip_addr, self._port)

    def get_local_ip(self):
        """获取本机ip。"""

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 53))
        ip = s.getsockname()[0]
        s.close()
        return ip

    def _update_meta(self, key, value):
        self._metadata[key] = value

    async def _renew(self):
        """发送心跳。"""

        url = "/apps/{}/{}".format(self._app._name, self.instance_id)
        return await self._app._author._do_req(url, method="PUT")

    @property
    def _str_(self):
        return json.dumps({
            "_app": self._app._name,
            "_is_register": self._is_register,
            "_is_heartbeat": self._is_heartbeat,
            "_metadata": self._metadata,
            "_dynamic_weight": self.dynamic_weight,
            "_lease_duration": self._lease_duration,
            "_lease_renewal_interval": self._lease_renewal_interval,
            "_ip_addr": self._ip_addr,
            "_port": self._port,
            "_hostname": self._hostname,
            "_instance_id": self.instance_id,
            "_health_check_url": self._health_check_url,
            "_status_page_url": self._status_page_url
        }, indent=4)