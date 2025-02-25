from abc import ABC
from .udf_base import UDFBase, get_constants
from ..utils.dataframe import list_to_PandasDF, pandasDF_to_list


class UDSF(UDFBase, ABC):
    """
    使用dataframe模式，set to set应当只允许dataframe模式
    """

    @property
    def udf_type(self):
        return "UDSF"

    def build_header(self, paths, types):
        # 用户直接返回dataframe，这个函数应当不需要
        pass

    def call_func(self, data, pos_args, kwargs):
        df = list_to_PandasDF(data)
        # 直接作为完整的dataframe进行处理，返回值为dataframe
        res = self.transform(df, *get_constants(pos_args), **kwargs)
        return pandasDF_to_list(res)
