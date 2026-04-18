# s2aManager Web

基于 `FastAPI + Jinja2 + 原生 JS` 的本机单机版 `sub2api` 号池管理网站，支持多站点切换。

当前提供三个页面 Tab，并且可以在页面顶部切换目标站点：

- `管理号池`
- `号池导入`
- `延时任务`

## 特性

- 按平台、类型、状态、分组、关键词等条件筛选账号
- 查看账号列表并调整列表分页大小
- 对筛选命中的所有分页账号执行批量修改
- 支持把批量修改保存为“X 分钟后”或“指定时间点”的延时任务
- 上传配置文件做预导入检查、格式转换和导入预览
- 支持 `credentials` / `extra` / 原始 JSON 高级编辑
- 网站自身用固定密码登录，密码支持环境变量或 `config.yaml`

## 启动

先准备虚拟环境：

```powershell
uv venv
```

安装依赖：

```powershell
uv sync
```

设置网站登录密码，二选一即可：

方式一，环境变量：

```powershell
$env:S2A_MANAGER_WEB_PASSWORD = "your-password"
```

方式二，写到 `config.yaml`：

```yaml
app:
  web_password: your-password
```

首次启动前，按需编辑根目录下的 `config.yaml`。
如果文件不存在，程序会自动生成默认配置。

启动：

```powershell
uv run uvicorn s2a_manager_web.main:app --host 127.0.0.1 --port 8000 --reload
```

浏览器访问：

```text
http://127.0.0.1:8000
```

## 配置说明

`config.yaml` 保存：

- `sites[].key`
- `sites[].name`
- `sites[].base_url`
- `sites[].admin_api_key`
- `sites[].timeout`
- `sites[].insecure`
- `default_site_key`
- `app.timezone`
- 页面默认分页与导入默认项

新版本推荐这样配置多站点：

```yaml
sites:
  - key: main
    name: 主站
    base_url: https://main.example.com
    admin_api_key: "admin-xxx"
    timeout: 30.0
    insecure: false
  - key: backup
    name: 备用站
    base_url: https://backup.example.com
    admin_api_key: "admin-yyy"
    timeout: 30.0
    insecure: false

default_site_key: main
```

如果你还在用旧格式的单站点 `sub2api:` 配置，程序也会自动兼容，不需要立刻迁移。

网站登录密码支持两种来源，优先级如下：

1. 环境变量 `S2A_MANAGER_WEB_PASSWORD`
2. `config.yaml` 中的 `app.web_password`

如果只是本机自用，直接写到 `config.yaml` 就可以。

延时任务的时间解析与页面展示使用 `app.timezone`，默认值为：

```yaml
app:
  timezone: Asia/Shanghai
```

在 Windows 环境下，如果使用 IANA 时区名且系统本身没有时区数据库，需要先执行一次：

```powershell
uv sync
```

这样会安装依赖里的 `tzdata`。当前版本即使没有 `tzdata`，也内置兜底支持 `Asia/Shanghai` 和 `UTC`。

## 支持的导入文件

当前优先支持这些内容格式：

- 标准 `DataPayload`
- 包含 `data` 的包装对象
- 简化 `accounts` / `proxies` JSON
- 单账号对象
- `auth snapshot`
- `.json` / `.yaml` / `.yml` / `.toml` 文件

## 说明

- 账号、分组、代理等业务数据不在本地重复存库，统一直接读取和写回当前选中的 `sub2api`
- `credentials.model_mapping` 通过 `credentials` JSON 编辑区维护
