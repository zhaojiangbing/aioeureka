# -*- coding  :   utf-8 -*-
# @Author     :   zhaojiangbing
# @File       :   client.py
# @Software   :   PyCharm


"""实现eureka 异步客户端。"""


import traceback
import json
import enum
import asyncio

from http import HTTPStatus
from .exc import EurekaException
from aiohttp import ClientSession, ClientTimeout
from log import manage_log
from .application import App


logger = manage_log.get_logger(__name__)


class StatusType(enum.Enum):
    """
    Available status types with eureka, these can be used
    for any `EurekaClient.register` call to pl
    """

    UP = 'UP'
    DOWN = 'DOWN'
    STARTING = 'STARTING'
    OUT_OF_SERVICE = 'OUT_OF_SERVICE'
    UNKNOWN = 'UNKNOWN'


class EurekaClient(object):
    """实现eureka客户端。"""

    def __init__(self, eureka_urls="http://localhost:8765", long_poll_interval=5, loop=None, timeout=60):
        """
        :param eureka_urls: str, eureka服务集群列表，用逗号隔开。
        :param long_poll_interval: int，单位秒，长轮询更新本地缓存。
        :param loop: 事件循环对象。
        :param timeout: int，单位秒，http请求超时总时间。
        """

        self._eureka_urls = eureka_urls.split(",")
        self._eureka_url = self._eureka_urls[0]
        self._long_poll_interval = long_poll_interval
        self._not_add_long_poll = True
        self._loop = loop or asyncio.get_event_loop()
        self._apps = {}  # 存放创建的app
        self._num = 0
        self._session = ClientSession(
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=ClientTimeout(total=timeout)
        )

    @property
    def number(self):
        self._num = (self._num + 1) % len(self._eureka_urls)
        return self._num

    def create_app(self, app_name):
        """创建一个app。

        :param app_name: str，app名字。
        :return: App实例。
        """

        if app_name in self._apps:
            return self._apps[app_name]

        app = App(self, app_name)
        return app

    def add_app(self, app):
        """添加app到_apps类字典。

        :param app: App对象。
        :return:
        """

        # self.remove_app(app)  # 如果app存在就删除
        if app._name not in self._apps:
            self._apps[app._name] = app

    def remove_app(self, app):
        """删除app。

        :param app: App对象。
        :return:
        """

        if app._name in self._apps:
            del self._apps[app._name]

    async def get_app(self, app_name, is_remote=False):
        """获取app。

        该方法会调用_long_poll方法，启动长轮询，实时更新本地缓存。

        :param app_name: str，app名字。
        :param is_remote: bool，是否从远端获取。
        :return: App对象 or None。
        """

        if self._not_add_long_poll:
            self._loop.create_task(self._long_poll())  # # 启动长轮询，实时更新本地缓存。
        if app_name not in self._apps or is_remote == True:
            await self._get_remote_app(app_name)  # 从远端获取app，会缓存到本地。
        return self._apps.get(app_name, None)

    async def _get_remote_app(self, app_name):
        """从远端获取app。

        :param app_name: str，app名字
        :return: App对象 or None
        """

        url = "/apps/{}".format(app_name)
        app = self.create_app(app_name)  # 从远端获取app就得创建app
        try:
            result = await self._do_req(url)
            for instance in result["application"]["instance"]:
                app.create_instance(
                    hostname=instance["hostName"],
                    ip_addr=instance["ipAddr"],
                    port=int(instance["port"]["$"]),
                    instance_id=instance["instanceId"],
                    metadata=instance["metadata"],
                    lease_duration=int(instance["leaseInfo"]["durationInSecs"]),
                    lease_renewal_interval=int(instance["leaseInfo"]["renewalIntervalInSecs"]),
                    status_page_url=instance["statusPageUrl"]
                    # health_check_url=None
                )
            self.add_app(app)  # 添加app
            return app
        except:
            logger.ainfo(traceback.format_exc())

    async def get_app_instance(self, app_name=None, instance_id=None, is_remote=False):
        """根据app名字，服务实例id获取服务实例，会缓存到本地。

        :param app_name: str，app名字
        :param instance_id: str，服务实例id。
        :param is_remote: bool，是否从远端获取。
        :return: Instance对象 or None。
        """

        app = await self.get_app(app_name, is_remote)
        if app:
            instance = await app.get_instance(instance_id, is_remote)
            return instance

        return None

    def register(self, instance):
        self._loop.create_task(self._register_(instance))

    async def _register_(self, instance):
        """注册service实例。

        :param instance: Instance，service实例对象。
        :return:
        """

        if not instance._is_register:
            instance._is_register = True
            try:
                payload = {
                    "instance": {
                        "instanceId": instance.instance_id,
                        "leaseInfo": {
                            "durationInSecs": instance._lease_duration,
                            "renewalIntervalInSecs": instance._lease_renewal_interval,
                        },
                        "port": {
                            "$": instance._port,
                            "@enabled": instance._port is not None,
                        },
                        "hostName": instance._hostname,
                        "app": instance._app._name,
                        "ipAddr": instance._ip_addr,
                        "vipAddress": instance._app._name,
                        "dataCenterInfo": {
                            "@class": "com.netflix.appinfo.MyDataCenterInfo",
                            "name": "MyOwn",
                        },
                    }
                }
                if instance._health_check_url is not None:
                    payload['instance']['healthCheckUrl'] = instance._health_check_url
                if instance._status_page_url is not None:
                    payload['instance']['statusPageUrl'] = instance._status_page_url
                if instance._metadata:
                    payload['instance']['metadata'] = instance._metadata

                url = "/apps/{}".format(instance._app._name)
                result = await self._do_req(url, method="POST", data=json.dumps(payload))  # 注册到远端
                logger.ainfo("register instance: {}".format(instance._str_))  # 打印注册的实列

                await self._heartbeat(instance)  # 启动心跳
            except:
                instance._is_register = False
                logger.ainfo(traceback.format_exc())

        return result

    async def deregister(self, instance):
        """注销应用实例。"""

        instance._is_heartbeat = False  # 取消心跳
        url = "/apps/{}/{}".format(instance._app._name, instance.instance_id)
        return await self._do_req(url, method="DELETE")

    async def set_status_override(self, instance, status: StatusType):
        """Sets the status override, note: this should generally only
        be used to pull services out of commission - not really used
        to manually be setting the status to UP falsely.

        :param instance: Instance对象。
        :param status: StatusType对象。
        :return:
        """

        url = "/apps/{}/{}/status?value={}".format(instance._app._name,
                                                   instance.instance_id,
                                                   status.value)
        return await self._do_req(url, method="PUT")

    async def remove_status_override(self):
        """Removes the status override."""
        url = "/apps/{}/{}/status".format(self._app_name,
                                          self.instance_id)
        return await self._do_req(url, method="DELETE")

    async def update_meta(self, instance, key, value):
        """更新instance的元数据。

        instance实例的元数据是k、v类型，v可以是任何类型。
        :param instance Instance，实例对象。
        :param key: str，元数据的key。
        :param value: 元数据的值，可以是任何类型。
        :return:
        """

        url = "/apps/{}/{}/metadata?{}={}".format(instance._app._name,
                                                  instance.instance_id,
                                                  key, value)
        result = await self._do_req(url, method="PUT")  # 更新远程
        instance._update_meta(key, value)  # 更新本地
        return result

    async def _get_remote_apps(self):
        """从远端获取所有app。"""

        url = "/apps"
        return await self._do_req(url)

    async def get_by_vip(self, app, vip_address=None):
        """Query for all instances under a particular vip address"""
        vip_address = vip_address or app._name
        url = "/vips/{}".format(vip_address)
        return await self._do_req(url)

    async def get_by_svip(self, app, svip_address=None):
        """Query for all instances under a particular secure vip address"""
        svip_address = svip_address or app._name
        url = "/vips/{}".format(svip_address)
        return await self._do_req(url)

    async def _do_req(self, path, method="GET", data=None):
        """http 请求方法。

        :param path: str，url path。
        :param method: str，http方法。
        :param data: json，请求的携带数据。
        :return:
        """

        url = self._eureka_url + path
        try:
            async with self._session.request(method, url, data=data) as resp:
                if 400 <= resp.status < 600:
                    status = resp.status
                    result = await resp.text()
                    logger.ainfo("http status: {}, http text: {}".format(status, result))
                    raise EurekaException(HTTPStatus(status), result)
                try:
                    result = await resp.json()
                except:
                    result = await resp.text()
                return result
        except Exception as e:
            self._eureka_url = self._eureka_urls[self.number]
            logger.ainfo(traceback.format_exc())

    async def _heartbeat(self, instance):
        """应用实例和eureka server 保持心跳。

        :param instance: Instance对象
        :return:
        """

        while instance._is_heartbeat:
            try:
                await instance._renew()  # 发送心跳
                logger.adebug("send heartbeat instance: {}".format(instance.instance_id))
            except:
                logger.ainfo(traceback.format_exc())
            await asyncio.sleep(instance._lease_renewal_interval)  # 睡眠 heartbeat_time 秒

    async def _long_poll(self):
        """定期更新本地缓存。"""

        self._not_add_long_poll = False  # 保证不重复启动长轮询
        while True:
            for app_name in list(self._apps.keys()):
                await self._get_remote_app(app_name)
            logger.adebug("{} long poll".format(self._eureka_url))
            await asyncio.sleep(self._long_poll_interval)

    @property
    def _str_(self):
        return json.dumps({
            "_eureka_url": self._eureka_url,
            "_long_poll_interval": self._long_poll_interval,
            "_not_add_long_poll": self._not_add_long_poll,
        }, indent=4)