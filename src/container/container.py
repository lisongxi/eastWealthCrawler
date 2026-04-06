"""
依赖注入容器
实现IoC模式用于管理组件依赖关系
"""

from typing import Any, Dict, Type, Optional, Callable
from functools import lru_cache
import inspect
import logging
from dataclasses import dataclass, field
from enum import Enum


class ComponentScope(Enum):
    """组件生命周期作用域"""

    SINGLETON = "singleton"  # 单例模式
    TRANSIENT = "transient"  # 瞬态模式


@dataclass
class ComponentRegistration:
    """组件注册信息"""

    component_type: Type  # 组件类型
    implementation: Optional[Type] = None  # 实现类型
    scope: ComponentScope = ComponentScope.SINGLETON  # 作用域
    factory: Optional[Callable] = None  # 工厂函数
    dependencies: Dict[str, Any] = field(default_factory=dict)  # 依赖关系


class DependencyContainer:
    """依赖注入容器"""

    def __init__(self):
        self._registrations: Dict[Type, ComponentRegistration] = {}  # 注册的组件
        self._instances: Dict[Type, Any] = {}  # 创建的实例
        self._logger = logging.getLogger(__name__)  # 日志记录器

    def register(
        self,
        component_type: Type,
        implementation: Optional[Type] = None,
        scope: ComponentScope = ComponentScope.SINGLETON,
        factory: Optional[Callable] = None,
    ):
        """在容器中注册组件"""
        if implementation is None:
            implementation = component_type

        if factory is None:
            factory = self._default_factory(implementation)

        self._registrations[component_type] = ComponentRegistration(
            component_type=component_type,
            implementation=implementation,
            scope=scope,
            factory=factory,
        )

    def register_singleton(
        self, component_type: Type, implementation: Optional[Type] = None
    ):
        """注册单例组件"""
        self.register(component_type, implementation, ComponentScope.SINGLETON)

    def register_transient(
        self, component_type: Type, implementation: Optional[Type] = None
    ):
        """注册瞬态组件"""
        self.register(component_type, implementation, ComponentScope.TRANSIENT)

    def register_instance(self, component_type: Type, instance: Any):
        """注册已创建的实例"""
        self._registrations[component_type] = ComponentRegistration(
            component_type=component_type,
            implementation=component_type,
            scope=ComponentScope.SINGLETON,
            factory=lambda container: instance,
        )
        self._instances[component_type] = instance

    def resolve(self, component_type: Type) -> Any:
        """解析组件实例"""
        if component_type not in self._registrations:
            raise ValueError(f"组件 {component_type} 未注册")

        registration = self._registrations[component_type]

        if registration.scope == ComponentScope.SINGLETON:
            if component_type not in self._instances:
                self._instances[component_type] = self._create_instance(registration)
            return self._instances[component_type]
        else:
            return self._create_instance(registration)

    def _create_instance(self, registration: ComponentRegistration) -> Any:
        """创建组件的新实例"""
        if registration.factory:
            return registration.factory(self)
        else:
            # 获取构造函数参数
            if registration.implementation is None:
                raise ValueError(
                    f"无法创建实例：implementation 为 None，component_type={registration.component_type}"
                )

            constructor = registration.implementation.__init__
            sig = inspect.signature(constructor)
            parameters = sig.parameters

            # 解析依赖
            dependencies = {}
            for name, param in parameters.items():
                if name == "self":
                    continue
                if param.annotation != inspect.Parameter.empty:
                    dependency_type = param.annotation
                    # 检查依赖类型是否有效
                    if dependency_type is None:
                        self._logger.warning(
                            f"参数 {name} 的类型注解为 None，跳过依赖解析"
                        )
                        continue
                    try:
                        dependencies[name] = self.resolve(dependency_type)
                    except ValueError as e:
                        self._logger.warning(
                            f"无法解析依赖 {name} ({dependency_type}): {e}"
                        )
                        # 尝试使用默认值
                        if param.default != inspect.Parameter.empty:
                            dependencies[name] = param.default

            # 创建实例
            try:
                instance = registration.implementation(**dependencies)
                return instance
            except TypeError as e:
                raise ValueError(
                    f"创建实例失败 {registration.component_type.__name__}: {e}. 依赖: {dependencies}"
                ) from e

    def _default_factory(self, implementation: Type) -> Callable:
        """创建实例的默认工厂方法"""

        def factory(container):
            return self._create_instance(
                ComponentRegistration(
                    component_type=implementation,
                    implementation=implementation,
                    factory=None,
                )
            )

        return factory

    def register_with_dependencies(
        self, component_type: Type, implementation: Type, dependencies: Dict[str, Any]
    ):
        """使用显式依赖注册组件"""

        def factory(container):
            resolved_dependencies = {}
            for name, dep_type in dependencies.items():
                resolved_dependencies[name] = container.resolve(dep_type)
            return implementation(**resolved_dependencies)

        self.register(component_type, implementation, factory=factory)


# 全局容器实例
container = DependencyContainer()


@lru_cache(maxsize=None)
def get_container() -> DependencyContainer:
    """获取全局依赖注入容器"""
    return container
