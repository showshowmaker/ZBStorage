# 运行说明

本文档整理项目的常用运行入口，全部使用脚本一键执行。

## 1. 运行前提

- 当前脚本均为 `bash` 脚本，推荐在 Linux、WSL 或 Git Bash 中执行。
- 根目录以下脚本默认使用：
  - `build/` 作为编译输出目录
  - `config/base.conf` 中的 `ROOT_PATH` 作为一键脚本和 demo 运行数据根目录
  - 如果 `ROOT_PATH` 为空，则回退到仓库下的 `.demo_run/`
- 主要脚本都在 [scripts](../scripts)。
- 示例 `txt` 路径树文件见 [examples/masstree_path_list_sample.txt](../examples/masstree_path_list_sample.txt)。

`config/base.conf` 格式：

```conf
ROOT_PATH=/data/zb_storage_demo
```

如果修改了 `ROOT_PATH`，以下目录都会随之迁移到该根路径下：

- `config/`
- `logs/`
- `pids/`
- `data/`
- `mnt/`

## 2. 一键编译整个项目

使用 [scripts/build_all.sh](../scripts/build_all.sh)。

```bash
bash scripts/build_all.sh build
```

常用变体：

```bash
# 仅配置
bash scripts/build_all.sh configure

# 清理后重新配置并编译
bash scripts/build_all.sh reconfigure

# 清理 build 目录
bash scripts/build_all.sh clean
```

常用环境变量：

```bash
BUILD_DIR=build
BUILD_TYPE=Release
CMAKE_GENERATOR=Ninja
JOBS=32
```

示例：

```bash
BUILD_TYPE=Release JOBS=32 bash scripts/build_all.sh build
```

## 3. 一键启动和关闭所有模块

使用 [scripts/start_demo_stack.sh](../scripts/start_demo_stack.sh)。

启动全部模块：

```bash
bash scripts/start_demo_stack.sh start
```

关闭全部模块：

```bash
bash scripts/start_demo_stack.sh stop
```

重启：

```bash
bash scripts/start_demo_stack.sh restart
```

查看状态：

```bash
bash scripts/start_demo_stack.sh status
```

该脚本会统一拉起：

- `scheduler_server`
- `real_node_server`
- `virtual_node_server`
- `mds_server`
- `zb_fuse_client`

运行目录默认在 `ROOT_PATH` 下：

- 运行数据目录：`<ROOT_PATH>/data`
- 配置目录：`<ROOT_PATH>/config`
- 日志目录：`<ROOT_PATH>/logs`
- 挂载目录：`<ROOT_PATH>/mnt`

如果 `ROOT_PATH` 为空，则等价于：

- 运行数据目录：`.demo_run/data`
- 配置目录：`.demo_run/config`
- 日志目录：`.demo_run/logs`
- 挂载目录：`.demo_run/mnt`

## 4. 根据 txt 文件创建 Masstree 模板

使用 [scripts/generate_masstree_template.sh](../scripts/generate_masstree_template.sh)。

前提：

1. 项目已编译完成。
2. demo stack 已启动。
3. 已准备好路径树 `txt` 文件。

命令：

```bash
bash scripts/generate_masstree_template.sh <template_id> <path_list_file> [repeat_dir_prefix]
```

示例：

```bash
bash scripts/start_demo_stack.sh start
bash scripts/generate_masstree_template.sh template-pathlist-100m examples/masstree_path_list_sample.txt copy
```

说明：

- 该脚本调用 `system_demo_tool --scenario=masstree_template`
- 服务端会根据 `txt` 路径树生成一个约 `1e8` 文件规模的模板
- 模板会记录目录树结构统计、层级统计、文件数、目录数、深度等信息

常用环境变量：

```bash
MDS_ADDR=127.0.0.1:9000
SCHEDULER_ADDR=127.0.0.1:9100
MASSTREE_VERIFY_INODE_SAMPLES=32
MASSTREE_VERIFY_DENTRY_SAMPLES=32
```

## 5. 根据模板导入一个新的命名空间

使用 [scripts/import_masstree_demo.sh](../scripts/import_masstree_demo.sh)。

命令：

```bash
bash scripts/import_masstree_demo.sh <namespace_count>
```

示例：

```bash
MASSTREE_TEMPLATE_ID=template-pathlist-100m \
MASSTREE_TEMPLATE_MODE=page_fast \
bash scripts/import_masstree_demo.sh 1
```

说明：

- 该脚本会连续导入多个 namespace
- 每个 namespace 都使用同一个模板
- namespace 名默认形如 `demo-ns-000001`

常用环境变量：

```bash
NAMESPACE_PREFIX=demo-ns
MASSTREE_TEMPLATE_ID=template-pathlist-100m
MASSTREE_TEMPLATE_MODE=page_fast
MASSTREE_VERIFY_INODE_SAMPLES=32
MASSTREE_VERIFY_DENTRY_SAMPLES=32
```

## 6. 根据模板生成 1000 亿文件规模元数据

推荐方式是：

1. 先生成一个约 `1e8` 文件的模板
2. 再重复导入 `1000` 个 namespace
3. 总体规模约为 `1000 * 1e8 = 1e11`

一键脚本使用 [scripts/run_import_1000yi_once.sh](../scripts/run_import_1000yi_once.sh)。

命令：

```bash
bash scripts/run_import_1000yi_once.sh 1000
```

这个脚本会自动：

1. 启动 demo stack
2. 可选地先根据 `txt` 生成模板
3. 连续导入 `1000` 个 namespace
4. 输出统一日志
5. 最后自动关闭 demo stack

如果模板已经存在，直接执行：

```bash
MASSTREE_TEMPLATE_ID=template-pathlist-100m \
MASSTREE_TEMPLATE_MODE=page_fast \
bash scripts/run_import_1000yi_once.sh 1000
```

如果希望脚本先根据 `txt` 自动生成模板，再开始导入：

```bash
MASSTREE_TEMPLATE_ID=template-pathlist-100m \
MASSTREE_TEMPLATE_MODE=page_fast \
MASSTREE_PATH_LIST_FILE=examples/masstree_path_list_sample.txt \
MASSTREE_REPEAT_DIR_PREFIX=copy \
bash scripts/run_import_1000yi_once.sh 1000
```

常用环境变量：

```bash
NAMESPACE_PREFIX=demo-ns
MASSTREE_TEMPLATE_ID=template-pathlist-100m
MASSTREE_TEMPLATE_MODE=page_fast
MASSTREE_PATH_LIST_FILE=examples/masstree_path_list_sample.txt
MASSTREE_REPEAT_DIR_PREFIX=copy
```

日志输出在：

- `.demo_run/logs/import_masstree_<namespace_count>ns_<timestamp>.log`

## 7. 运行 demo 演示程序

交互式 demo 使用 [scripts/run_system_demo.sh](../scripts/run_system_demo.sh)。

前提：

1. 项目已编译完成。
2. demo stack 已启动。

命令：

```bash
bash scripts/run_system_demo.sh
```

该脚本会启动交互式 `system_demo_tool`，进入菜单界面。

补充说明：

- `Masstree Query Demo` 默认使用 `query_mode=random_path_lookup`
- `random_path_lookup` 会在客户端按需读取 `MASSTREE_PATH_LIST_FILE`
- 每次查询会随机取一行；如果该行是目录，则继续向下扫描到该目录下第一个文件
- 然后拼出真实查询路径：`<path_prefix>/<copy_xxxxxx>/<relative_file_path>`
- 统计时延时，只计算 `Lookup(path)` RPC，从发送文件路径开始，到收到 inode 信息结束
- 不计算客户端解析 `txt` 的时间
- 因为查询要读取当前导入得到的 `manifest_path`，所以在同一个 demo 进程里应先执行一次 `5`，再执行 `6`

## 8. 一键展示 demo 程序的各个功能

使用 [scripts/run_demo_showcase.sh](../scripts/run_demo_showcase.sh)。

命令格式：

```bash
bash scripts/run_demo_showcase.sh [template_id] [path_list_file]
```

示例 1：模板已经存在，直接展示完整 demo：

```bash
bash scripts/run_demo_showcase.sh template-pathlist-100m
```

示例 2：先根据 `txt` 自动生成模板，再展示完整 demo：

```bash
bash scripts/run_demo_showcase.sh template-pathlist-100m examples/masstree_path_list_sample.txt
```

该脚本会自动：

1. 启动 demo stack
2. 执行 `system_demo_tool --scenario=all`
3. 如果传入了 `path_list_file`，会先走模板生成，再走模板导入和查询
4. 输出演示日志
5. 最后自动关闭 demo stack

日志输出在：

- `.demo_run/logs/demo_showcase_<timestamp>.log`

## 9. demo 交互菜单功能说明

进入交互式 demo 后，可用功能如下：

- `1`：环境健康检查
- `2`：全局统计检查
- `3`：真实节点 POSIX 读写演示
- `4`：虚拟节点 POSIX 读写演示
- `5`：按模板导入一个 Masstree namespace
- `10`：根据 `txt` 路径树生成 Masstree 模板
- `6`：Masstree 随机查询
- `7`：执行完整测试集
- `8`：查看上一次结果
- `9`：帮助
- `0`：退出

典型交互命令：

```text
10 template_id=template-pathlist-100m path_list_file=examples/masstree_path_list_sample.txt repeat_dir_prefix=copy
5 namespace=demo-ns generation=gen-001 template_id=template-pathlist-100m template_mode=page_fast
6 n=10
6 n=1000 query_mode=random_path_lookup
6 n=1000 query_mode=random_inode
7
```

其中：

- `6 n=10` 等价于 `6 n=10 query_mode=random_path_lookup`
- `random_path_lookup` 是默认模式，适合测真实路径逐层查找时延
- `random_inode` 直接走随机 inode 查询，不走客户端路径拼接

## 10. 推荐完整流程

### 10.1 手工分步执行

```bash
# 1. 编译
bash scripts/build_all.sh build

# 2. 启动
bash scripts/start_demo_stack.sh start

# 3. 生成模板
bash scripts/generate_masstree_template.sh template-pathlist-100m examples/masstree_path_list_sample.txt copy

# 4. 导入 1 个 namespace 验证
MASSTREE_TEMPLATE_ID=template-pathlist-100m MASSTREE_TEMPLATE_MODE=page_fast \
bash scripts/import_masstree_demo.sh 1

# 5. 关闭
bash scripts/start_demo_stack.sh stop
```

### 10.2 一键导入 1000 亿规模

```bash
MASSTREE_TEMPLATE_ID=template-pathlist-100m \
MASSTREE_TEMPLATE_MODE=page_fast \
MASSTREE_PATH_LIST_FILE=examples/masstree_path_list_sample.txt \
MASSTREE_REPEAT_DIR_PREFIX=copy \
bash scripts/run_import_1000yi_once.sh 1000
```

### 10.3 一键完整功能演示

```bash
bash scripts/run_demo_showcase.sh template-pathlist-100m examples/masstree_path_list_sample.txt
```
