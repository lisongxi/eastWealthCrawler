# 东方财富股票数据爬虫

## 项目简介

本项目使用 Python 的 `asyncio` 异步编程框架和 `aiohttp` 异步HTTP客户端，实现了高效的股票数据爬取系统。项目采用企业级分层架构设计，支持事件驱动架构，具有良好的可扩展性和可维护性。

## 技术特点

- **异步IO**: 基于 `asyncio` 实现高效的异步爬取
- **并发请求**: 使用 `aiohttp` 进行并发HTTP请求
- **事件驱动**: 支持事件驱动架构
- **分层架构**: 表现层、应用层、领域层、基础设施层分离
- **设计模式**: 策略模式、依赖注入、数据管道、事件总线
- **健康监控**: 内置系统健康检查和性能监控
- **统一错误处理**: 标准化的错误处理系统

## 功能

- 爬取股票板块历史资金流数据
- 爬取股票板块历史价格K线数据
- 爬取5000+个股的历史价格数据
- 支持数据保存到MySQL数据库
- 支持全量同步和增量同步

## 环境要求

- Python 3.7+
- MySQL 5.7+
- 虚拟环境（推荐使用）

## 本地启动完整步骤

### 步骤 1：检查 MySQL 服务状态

确保 MySQL 服务已启动：

**macOS:**
```bash
# 使用 Homebrew 安装的 MySQL
brew services start mysql

# 或者使用系统自带的 MySQL
sudo /usr/local/mysql/support-files/mysql.server start
```

**Linux:**
```bash
sudo systemctl start mysql
# 或
sudo service mysql start
```

**Windows:**
```bash
# 在服务管理器中启动 MySQL 服务
# 或使用命令
net start mysql
```

**验证 MySQL 是否运行：**
```bash
mysql -u root -p
# 输入密码后，看到 MySQL 命令行提示符即为成功
```

### 步骤 2：配置数据库连接

编辑 `settings/settings.yaml` 文件，修改MySQL连接信息：

```yaml
eastWealth:
  mysql:
    database: eastwealthcrawler  # 数据库名称
    host: 127.0.0.1              # 数据库主机
    port: 3306                   # 数据库端口
    user: root                   # 数据库用户名
    password: "your_password"    # 数据库密码（请替换为实际密码）
```

### 步骤 3：创建并激活虚拟环境

**创建虚拟环境：**

```bash
# Python 3
python3 -m venv venv

# 或使用 python
python -m venv venv
```

**激活虚拟环境：**

**macOS/Linux:**
```bash
source venv/bin/activate
```

**Windows:**
```powershell
# PowerShell
venv\Scripts\Activate.ps1

# 或使用 CMD
venv\Scripts\activate.bat
```

### 步骤 4：安装项目依赖

```bash
# 确保虚拟环境已激活
# 升级 pip 到最新版本
pip install --upgrade pip

# 安装项目依赖
pip install -r requirements.txt
```

**如果安装速度慢，使用国内镜像源：**

```bash
# 使用清华大学镜像
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 步骤 5：初始化数据库

```bash
# 确保虚拟环境已激活
python init_db.py
```

### 步骤 6：运行爬虫

```bash
python main.py
```

## 运行模式配置

在 `settings/settings.yaml` 中配置运行模式：

```yaml
sync:
  mode: full  # full (全量) 或 incremental (增量)
  incremental_time: "20:00"  # 增量同步时间
  start_date: "2019-01-01"  # 数据开始日期
```

### 全量模式

- 只运行一次，然后进入等待状态
- 适合首次运行或重新爬取所有数据

### 增量模式

- 每天在指定时间自动执行
- 适合日常数据更新

## 核心文件说明

### 主要入口
- `main.py` - 主入口文件，使用事件驱动架构
- `init_db.py` - 数据库初始化脚本

### 配置和数据库
- `database.py` - 数据库连接管理（使用Peewee ORM）
- `settings/settings.yaml` - 配置文件

### 板块爬虫
- `models.block/blockCrawl.py` - 板块数据爬虫核心实现（保留完整模式需要的函数）
- `models.block/blockDM.py` - 板块数据模型定义

### 个股爬虫
- `models.stock/stockDM.py` - 个股数据模型定义

### 企业级架构（src目录）
- `src/application/` - 应用层服务（爬虫编排器、服务层）
- `src/domain/` - 领域层（实体、值对象）
- `src/strategies/` - 策略层（爬虫策略模式实现）
- `src/pipeline/` - 数据管道层（验证、转换、富化、存储）
- `src/events/` - 事件驱动层（事件总线）
- `src/container/` - 依赖注入层（IoC容器）
- `src/common/` - 通用组件（错误处理、健康监控）
- `src/config/` - 配置管理（集中式配置系统）

## 数据库表结构

### 板块资金流表 (t_block_capital_flow)

| 字段名 | 类型 | 说明 |
|--------|------|------|
| b_date | Date | 日期 |
| block_code | String | 板块代码 |
| block_name | String | 板块名称 |
| main_net_inflow | Float | 主力净流入 |
| large_net_inflow | Float | 大单净流入 |
| mid_net_inflow | Float | 中单净流入 |
| small_net_inflow | Float | 小单净流入 |
| d_closing_price | Float | 当日收盘价 |
| d_quote_change | Decimal | 当日涨跌幅 |

### 板块历史价格表 (t_block_price)

| 字段名 | 类型 | 说明 |
|--------|------|------|
| b_date | Date | 日期 |
| block_code | String | 板块代码 |
| block_name | String | 板块名称 |
| open_price | Float | 开盘价 |
| close_price | Float | 收盘价 |
| top_price | Float | 最高价 |
| low_price | Float | 最低价 |
| turnover | Float | 成交量 |
| transaction | Float | 成交额 |
| amplitude | Decimal | 振幅 |
| quote_change | Decimal | 涨跌幅 |

### 个股K线数据表 (t_stock_kline)

| 字段名 | 类型 | 说明 |
|--------|------|------|
| s_date | Date | 日期 |
| stock_code | String | 股票代码 |
| stock_name | String | 股票名称 |
| open_price | Float | 开盘价 |
| close_price | Float | 收盘价 |
| top_price | Float | 最高价 |
| low_price | Float | 最低价 |
| turnover | Float | 成交量 |
| transaction | Float | 成交额 |
| amplitude | Decimal | 振幅 |
| quote_change | Decimal | 涨跌幅 |

## 架构设计

### 运行模式

项目采用**完整模式**运行，使用企业级架构：

#### 事件驱动架构

```
┌─────────────────────────────────────────────────────────┐
│                  表现层               │
│  ┌─────────────┐    ┌───────────────────────────────┐  │
│  │   CLI       │    │           健康端点           │  │
│  └─────────────┘    └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│                  应用层             │
│  ┌────────────────────────────────────────────────┐  │
│  │              爬虫编排器          │  │
│  └────────────────────────────────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐              │
│  │板块爬虫服务   │  │股票爬虫服务   │              │
│  └──────────────┘  └──────────────┘              │
│  ┌────────────────────────────────────────────────┐  │
│  │              数据管道              │  │
│  │  ┌─────┐ ┌──────┐ ┌──────┐ ┌──────┐         │  │
│  │  │验证 │ │转换  │ │富化  │ │存储  │         │  │
│  │  └─────┘ └──────┘ └──────┘ └──────┘         │  │
│  └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│                  领域层                │
│  ┌────────────────────────────────────────────────┐  │
│  │         实体和值对象          │  │
│  └────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────┐  │
│  │         领域服务            │  │
│  └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│              基础设施层          │
│  ┌────────────────────────────────────────────────┐  │
│  │        横切关注点              │  │
│  │  事件总线, 依赖注入容器, 配置管理           │  │
│  │  错误处理, 健康监控, 日志                  │  │
│  └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 核心架构模式

#### 1. 依赖注入 (IoC)
**位置**: `src/container/container.py`

**优势**:
- 消除组件间的硬编码依赖
- 使组件更易于测试和维护
- 支持单例和瞬态生命周期

#### 2. 策略模式
**位置**: `src/strategies/strategies.py`

**优势**:
- 允许在运行时选择不同的爬虫算法
- 消除算法选择的条件语句
- 便于添加新的爬虫策略

#### 3. 事件驱动架构 (EDA)
**位置**: `src/events/event_bus.py`

**优势**:
- 通过事件发布/订阅解耦组件
- 支持异步通信
- 支持复杂工作流和通知

#### 4. 数据管道模式
**位置**: `src/pipeline/data_pipeline.py`

**优势**:
- 提供一致的数据处理流程
- 将处理关注点分离为离散步骤
- 使数据转换更易于维护

#### 5. 分层架构

**层次**:
1. **表现层**: CLI 和其他接口
2. **应用层**: 用例和编排
3. **领域层**: 核心业务逻辑和实体
4. **基础设施层**: 外部实现和适配器

## 技术栈

### 核心技术
- **Python**: 3.7+
- **异步框架**: asyncio (Python标准库)
- **异步HTTP**: aiohttp 3.8+
- **ORM**: Peewee 3.16+
- **配置管理**: PyYAML 6.0+, Pydantic 2.0+
- **数据库**: MySQL 5.7+

### 技术亮点
- **异步爬取**: 使用 `asyncio` + `aiohttp` 实现高效的并发爬取
- **事件循环**: 异步任务调度和事件驱动架构
- **企业级架构**: 分层架构、依赖注入、事件驱动

## 常见问题

### 1. 数据库连接失败

**问题**: `pymysql.err.OperationalError: Access denied for user`

**解决**: 检查 `settings/settings.yaml` 中的数据库用户名和密码是否正确

### 2. 端口被占用

**问题**: `Can't connect to MySQL server on '127.0.0.1:3306'`

**解决**:
- 确认MySQL服务已启动
- 检查 `settings/settings.yaml` 中的端口配置

### 3. 依赖包安装失败

**问题**: 某些包安装失败

**解决**: 使用国内镜像源

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 4. 没有数据返回

**问题**: 运行成功但数据库为空

**解决**:
- 检查网络连接
- 确认东方财富网站可访问
- 查看日志文件排查具体错误

## 许可证

本项目仅供学习交流使用，请勿用于商业用途。

## 致谢

感谢东方财富网提供数据接口。
