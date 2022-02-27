# -*- coding  :   utf-8 -*-
# @Software   :   PyCharm


"""实现负载均衡。"""


import random


class LoadBalance(object):
    def __init__(self, app):
        """

        :param app: App对象。
        """

        self._app = app
        self._poll_count = 0

    def get_instance(self, instance_id):
        """获取本地实例。

        :param instance_id: str，实例id。
        :return:
        """

        instance = self._app._instances[instance_id]
        return instance

    def _random_get_instance(self):
        """随机获取instance。"""

        instance_ids = sorted(self._app._instances.keys())  # 实时获取实例id列表并且排序
        number = random.randint(0, len(instance_ids)-1)  # 随机选择一个number

        instance = self.get_instance(instance_ids[number])
        return instance

    def _poll_get_instance(self):
        """轮询获取instance。"""

        instance_ids = sorted(self._app._instances.keys())  # 实时获取实例id列表并且排序
        length = len(instance_ids)
        if self._poll_count >= length:
            self._poll_count = 0  # 清零
        number = self._poll_count % length  # 取模
        self._poll_count += 1

        instance = self.get_instance(instance_ids[number])
        return instance

    def _poll_weight_get_instance(self):
        """加权轮询获取instance。"""

        total = 0
        max_dynamic_weight_instance = None  # 用于记录动态权重最大的实例
        for _id, instance in self._app._instances.items():
            if max_dynamic_weight_instance is None:
                max_dynamic_weight_instance = instance

            weight = int(instance._metadata["weight"])
            instance.dynamic_weight += weight  # 修改动态权重值

            if instance.dynamic_weight > max_dynamic_weight_instance.dynamic_weight:
                max_dynamic_weight_instance = instance  # 找出动态权重最大的实例

            total += weight  # 权重累计求和

        max_dynamic_weight_instance.dynamic_weight -= total  # 对选出实例的动态权重减去总权重

        return max_dynamic_weight_instance