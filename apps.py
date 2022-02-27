# -*- coding  :   utf-8 -*-
# @Author     :   zhaojiangbing
# @File       :   apps.py
# @Software   :   PyCharm


"""该模块实现服务的注册和发现接口。"""


import enum

from aiohttp import ClientSession, ClientTimeout, TCPConnector


class Strategy(enum.Enum):
    """负载均衡策略。"""

    _random = "_random_get_instance"  # 随机
    _poll = "_poll_get_instance"  # 轮询
    _poll_weight = "_poll_weight_get_instance"  # 加权轮询


class RegisterApp(object):
    """该类实现服务注册接口类。

    Attribute:
        _driver: 该类_driver是需要提供的驱动。
    """

    _driver = None  # 注册、发现服务的驱动对象或者客户端对象。

    @classmethod
    def set_driver(cls, driver):
        cls._driver = driver

    def register_instance(self, instance, *args, **kwargs):
        """注册instance。

        :param instance: 要被注册的服务实例对象。
        """

        return self._driver.register(instance, *args, **kwargs)

    async def deregister_instance(self, instance, *args, **kwargs):
        """注销instance。

        :param instance: 要被注销的服务实例对象。
        """

        return await self._driver.deregister(instance, *args, **kwargs)

    async def update_meta(self, instance, key, value):
        """更新instance的元数据。

        实例的元数据是k、v类型，v可以是任何类型。

        :param instance: 要被更新元数据的实例对象。
        :param key: str，元数据的key。
        :param value: 元数据的值，可以是任何类型。
        :return:
        """

        return await self._driver.update_meta(instance, key, value)

    def create_app(self, app_name):
        """创建app。"

        :param app_name: str，app名字。
        :return:
        """

        return self._driver.create_app(app_name)


class DiscoverApp(object):
    """服务发现接口类。

    Attribute:
        _driver: 该类_driver是需要提供的驱动，驱动要支持负载均衡。
    """

    _driver = None  # 注册、发现服务的驱动对象或者客户端对象。

    def __init__(self, app_name=None, strategy=Strategy._random, protocol="http"):
        """
        :param app_name: str，应用名字。
        :param strategy: 是个枚举值，取值范围参考Strategy的属性。
        :param protocol: str，http或者https。
        """

        self._app_name = app_name
        self._strategy_func = strategy.value
        assert protocol in ("http", "https"), "protocol must be 'http' or 'https'"
        self._protocol = protocol
        self._session = ClientSession(timeout=ClientTimeout(connect=2, sock_connect=1, sock_read=2),
                                      connector=TCPConnector(limit=1024))

    @classmethod
    def set_driver(cls, driver):
        cls._driver = driver

    def set_session(self, session):
        """设置http请求的会话对象

        :param session: ClientSession，http请求的会话对象
        :return:
        """

        assert isinstance(session, ClientSession), "session type error"
        self._session = session

    async def request(self, path=None, method="GET", is_hostname=False, **kwargs):
        """

        :param path: str，rest api的path。
        :param method: str，http的方法。
        :param is_hostname: bool，如果为True，会按主键名字拼接地址，否则按ip拼接地址。
        :param kwargs: 包括http协议常用字段。
        :return:
        """

        app  = await self.get_app(self._app_name)  # 获取应用
        load_blance_func = getattr(app.load_balance, self._strategy_func)  # 拿到负载均衡实例的函数
        instance = load_blance_func()  # 获取实例
        if is_hostname:
            addr = "{}:{}".format(instance._hostname, instance._port)  # 根据主机名获取地址
        else:
            addr = "{}:{}".format(instance._ip_addr, instance._port)  # 根据ip获取地址
        url = "{}://{}/{}".format(self._protocol, addr, path.lstrip("/"))  # 拼接url
        async with self._session.request(method=method.upper(), url=url, **kwargs) as resp:
            result = await resp.text()
            status = resp.status
            return status, result

    async def get_app(self, app_name, is_remote=False):
        """根据应用名字获取app。

        :param app_name: str，应用名称。
        :param is_remote: bool，是否从远端获取。
        :return: dict
        """

        return await self._driver.get_app(app_name, is_remote)

    async def get_app_instance(self, app_name, instance_id, is_remote=False):
        """根据应用名字和实例id获取实例。

        :param app_name: str，应用名称。
        :param instance_id: str，instance的id，每一个应用有多个instance。
        :param is_remote: bool，是否从远端获取。
        :return: dict
        """

        return await self._driver.get_app_instance(app_name, instance_id, is_remote)