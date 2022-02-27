# -*- coding  :   utf-8 -*-
# @Author     :   zhaojiangbing
# @File       :   reg_dis_demo.py
# @Software   :   PyCharm


from client import EurekaClient
from apps import RegisterApp, DiscoverApp, Strategy


#  一、注册服务-------------------------------------------------



app_name = "noansercall_mq"  # 开发人员从配置获取，注册实例的应用
server_port = 8080  # 开发人员从配置获取，注册实例的ip
register_server_url = "http://172.16.1.54:8080"  # 开发人员从配置获取，eureka server的地址
register_heartbeat_interval = 10  # 开发人员从配置获取，实例续约的周期
register_duration_interval = 30  # 开发人员从配置获取，在这个时间类没有续约，eureka server就认为实例挂了

# 1、首先创建驱动对象
eureka = EurekaClient(eureka_url=register_server_url)

# 2、用驱动对象创建一个应用
project_app = eureka.create_app(app_name)  # 创建一个应用(app)，其实就代表本项目应用

# 3、用应用对象创建一个实例
project_instance = project_app.create_instance(port=server_port, lease_renewal_interval=register_heartbeat_interval,
                                               lease_duration=register_duration_interval, metadata={"weight": 1})  # 创建一个应用实例(instance)，其实就代表本项目服务
"""这里在创建实例添加了元数据（metadata），负载均衡的加权轮询算法需要借助元数据中的weight字段，如果没提供默认为1，
上面提供了注册实例的端口，实例的ip在没提供时取部署机器的ip，实例id默认为：ip:port。
"""

# 4、在创建注册工具前，先设置注册工具的驱动
RegisterApp.set_driver(eureka)

# 5、创建一个注册工具
register = RegisterApp()

# 6、注册实例到远端
register.register_instance(project_instance)

# 7、注销实例
register.deregister_instance(project_instance)

# 8、更新实例的元数据
"""实例的元数据是json object，只支持最外层的值修改"""
register.update_meta(project_instance, key="weight", value=3)



#  二、服务发现-------------------------------------------------



app_name = "noansercall_model"  # 开发人员从配置获取
discover_long_poll_interval = 5  # 开发人员从配置获取，轮询从远端获取数据到本地的周期
strategy = Strategy._random  # 负载均衡的策略，默认是随机

# 1、首先创建驱动对象
eureka = EurekaClient(eureka_url=register_server_url, long_poll_interval=discover_long_poll_interval)

# 2、在使用发现服务工具前，也必须先设置驱动
DiscoverApp.set_driver(eureka)

# 3、创建一个发现服务工具
discover = DiscoverApp(app_name=app_name, strategy=strategy)  # 创建对象的时候要指定应用和负载均衡策略

# 4、请求应用rest api
discover.request(path="/asr/processapi", method="GET", data={})  # mothod 必须大写
