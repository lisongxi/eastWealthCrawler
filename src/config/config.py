"""
集中式配置管理系统
"""

import os
import json
import yaml
from typing import Any, Dict, Optional, Type, TypeVar, Generic
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import logging
from functools import lru_cache

from pydantic import BaseModel, ValidationError, validator
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

T = TypeVar("T", bound=BaseModel)


class ConfigurationSource(Enum):
    """配置源类型"""

    ENVIRONMENT = "environment"  # 环境变量
    FILE = "file"  # 文件
    DEFAULT = "default"  # 默认值


@dataclass
class ConfigurationSourceInfo:
    """配置源信息"""

    source_type: ConfigurationSource  # 源类型
    path: Optional[str] = None  # 文件路径 path
    priority: int = 0  # 优先级


class ConfigurationManager(Generic[T]):
    """集中式配置管理器"""

    def __init__(self, config_model: Type[T], sources: Optional[list] = None):
        self.config_model = config_model  # 配置模型类型
        self.sources = sources or []  # 配置源列表
        self._config: Optional[T] = None  # 缓存的配置实例
        self._logger = logging.getLogger(__name__)  # 日志记录器

    def add_source(
        self,
        source_type: ConfigurationSource,
        path: Optional[str] = None,
        priority: int = 0,
    ):
        """添加配置源"""
        self.sources.append(ConfigurationSourceInfo(source_type, path, priority))
        self.sources.sort(key=lambda x: x.priority, reverse=True)

    def load_configuration(self) -> T:
        """从所有源加载配置"""
        if self._config is not None:
            return self._config

        config_data = {}

        for source in self.sources:
            try:
                if source.source_type == ConfigurationSource.ENVIRONMENT:
                    env_data = self._load_from_environment()
                    config_data = self._deep_merge(config_data, env_data)
                elif source.source_type == ConfigurationSource.FILE:
                    if source.path:
                        file_data = self._load_from_file(source.path)
                        config_data = self._deep_merge(config_data, file_data)
            except Exception as e:
                self._logger.warning(f"从 {source.source_type} 加载配置失败: {e}")

        # 应用模型中的默认值
        default_data = self._get_default_values()
        config_data = self._deep_merge(default_data, config_data)

        # 验证并创建配置对象
        try:
            self._config = self.config_model(**config_data)
            return self._config
        except ValidationError as e:
            self._logger.error(f"配置验证失败: {e}")
            raise

    def _load_from_environment(self) -> Dict[str, Any]:
        """从环境变量加载配置"""
        config_data = {}

        # 使用 model_fields（Pydantic V2）或 __fields__（Pydantic V1）
        try:
            model_fields = self.config_model.model_fields
        except AttributeError:
            model_fields = self.config_model.__fields__

        for field_name, field in model_fields.items():
            env_var = f"{self.config_model.__name__.upper()}_{field_name.upper()}"
            if env_var in os.environ:
                value = os.environ[env_var]
                # 获取字段类型
                field_type = getattr(field, "annotation", None)
                if field_type is None:
                    continue
                # 转换为适当类型
                if field_type is bool:
                    value = value.lower() in ("true", "1", "t", "y", "yes")
                elif field_type is int:
                    value = int(value)
                elif field_type is float:
                    value = float(value)
                config_data[field_name] = value
        return config_data

    def _load_from_file(self, file_path: str) -> Dict[str, Any]:
        """从文件加载配置"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"配置文件未找到: {file_path}")

        with open(path, "r", encoding="utf-8") as f:
            if path.suffix in (".yml", ".yaml"):
                return yaml.safe_load(f)
            elif path.suffix == ".json":
                return json.load(f)
            else:
                raise ValueError(f"不支持的配置文件格式: {path.suffix}")

    def _get_default_values(self) -> Dict[str, Any]:
        """从配置模型获取默认值"""
        defaults = {}

        # 使用 model_fields（Pydantic V2）或 __fields__（Pydantic V1）
        try:
            model_fields = self.config_model.model_fields
        except AttributeError:
            model_fields = self.config_model.__fields__

        for field_name, field in model_fields.items():
            # Pydantic V2: 使用 get_default()（自动处理 default 和 default_factory）
            if hasattr(field, "get_default"):
                default = field.get_default()
                if default is not None:
                    defaults[field_name] = default
            # Pydantic V1 fallback
            elif hasattr(field, "default") and field.default is not None:
                defaults[field_name] = field.default
        return defaults

    def _deep_merge(
        self, base: Dict[str, Any], update: Dict[str, Any]
    ) -> Dict[str, Any]:
        """深度合并两个字典"""
        result = base.copy()
        for key, value in update.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def get(self, key: str) -> Any:
        """通过键获取配置值"""
        if self._config is None:
            self.load_configuration()

        keys = key.split(".")
        value = self._config
        for k in keys:
            if hasattr(value, k):
                value = getattr(value, k)
            elif isinstance(value, dict) and k in value:
                value = value[k]
            else:
                raise KeyError(f"配置键未找到: {key}")
        return value

    def reload(self):
        """重新加载配置"""
        self._config = None
        return self.load_configuration()


class ConfigurationFactory:
    """创建配置管理器的工厂类"""

    @staticmethod
    def create_config_manager(
        config_model: Type[T], config_dir: str = "config"
    ) -> ConfigurationManager[T]:
        """使用默认源创建配置管理器"""
        manager = ConfigurationManager(config_model)

        # 添加环境变量作为最高优先级
        manager.add_source(ConfigurationSource.ENVIRONMENT, priority=100)

        # 如果存在则添加YAML文件
        yaml_path = os.path.join(config_dir, f"{config_model.__name__.lower()}.yaml")
        if os.path.exists(yaml_path):
            manager.add_source(ConfigurationSource.FILE, yaml_path, priority=50)

        # 如果存在则添加JSON文件
        json_path = os.path.join(config_dir, f"{config_model.__name__.lower()}.json")
        if os.path.exists(json_path):
            manager.add_source(ConfigurationSource.FILE, json_path, priority=40)

        return manager


# 全局配置缓存
_config_cache: Dict[Type, Any] = {}


def get_config(config_model: Type[T]) -> T:
    """获取缓存的配置实例"""
    if config_model not in _config_cache:
        manager = ConfigurationFactory.create_config_manager(config_model)
        _config_cache[config_model] = manager.load_configuration()
    return _config_cache[config_model]


def clear_config_cache():
    """清除配置缓存"""
    global _config_cache
    _config_cache = {}
