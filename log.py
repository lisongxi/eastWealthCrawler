"""日志系统
"""
import os.path
from enum import Enum
from datetime import datetime
import pipe
from functools import reduce
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from typing import Union, Optional
import json

from config import get_settings

settings = get_settings()  # 获取settings配置文件

# 日期格式化模板
__FREQS__ = {'month': '%Y%m', 'day': '%Y%m%d', 'hour': '%Y%m%d%H'}

# 自定义 日志频率
freq = __FREQS__[settings.log.freq.value]  # 通过 Settings类 找到 Freq类 的值


class LogType(Enum):
    """日志类型
    """
    run_error = '运行出错'
    save_file = '爬取数据成功'


class Log:
    """日志处理
    """
    _file_format = '.txt'
    _intro_template = "By {username}, at {datetime}, content: \n"

    def __init__(self, *, path: str, fq: str = freq, useUTC: bool = settings.log.useUTC, logType: LogType):
        """初始化
        生成文件名后缀
        Args:
            fq：日志频率
            fmt：写入文件格式，默认为 txt
            useUTC：是否使用UTC时间
            path：文件夹路径
        """
        now = datetime.utcnow() if useUTC else datetime.now()  # 当前时间

        file_suffix = now.strftime(fq)  # 时间格式转化

        self.file = os.path.join(path, logType.value + '_' + file_suffix + self._file_format)  # 自动拼接路径
        self.datetime = now.isoformat()  # 将一个datetime对象转换为ISO 8601格式的字符串。

    @classmethod
    def _trace_error(cls, err: Exception) -> str:
        """追溯异常
        """
        err_name = (
                err.__class__.mro()[:-2] |
                pipe.map(lambda x: str(x)) |
                pipe.map(lambda x: x.lstrip("<class '").rstrip("'>")) |
                pipe.map(lambda x: x.split('.')[-1]) |
                pipe.Pipe(lambda x: reduce(lambda a, b: a + ' ' + u'\u2190' + ' ' + b, x))
        )
        return err_name

    @classmethod
    def _split_model(cls, model: BaseModel) -> tuple:
        """拆解模型
        """
        keys = model.__fields__.keys()
        values = jsonable_encoder(model).values()
        return keys, values

    def add_txt_row(self, *, username: Optional[str] = None, content: Union[Exception, BaseModel, str]):
        """添加一行日志

        Args:
            username：用户账号
            content：写入内容，格式为字符串、pydantic.BaseModel模型、异常。如果是模型，则遍历每个字段；如果是异常，则追溯所有父类
        """

        if username is None:
            username = settings.sysAdmin
        if isinstance(content, str):
            text = content
        elif isinstance(content, Exception):
            text = self._trace_error(err=content) + '\n' + '-' * 200 + (f'\n%s' % content)
        elif isinstance(content, BaseModel):
            text = json.dumps(
                jsonable_encoder(content.dict(exclude_unset=True)),
                indent=4, ensure_ascii=False
            )
        with open(self.file, 'at') as f:  # 追加记录，文件不存在则创建
            f.write(
                self._intro_template.format(username=username, datetime=self.datetime)
            )
            f.write('-' * 200 + '\n')
            f.write(text + '\n')
            f.write('=' * 200 + '\n\n')
