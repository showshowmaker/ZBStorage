那就不用“创建 1000 个模板”。

你的目标对应的是：

1. 先创建 `1` 个模板
2. 再用这个模板导入 `1000` 个 namespace

直接用现成的一键脚本就行：

```bash
nohup env \
  MASSTREE_TEMPLATE_ID=template-01-tijian \
  MASSTREE_TEMPLATE_MODE=page_fast \
  MASSTREE_PATH_LIST_FILE=/mnt/md0/wjh/trees/tree1_tijian.txt \
  MASSTREE_REPEAT_DIR_PREFIX=copy \
  bash scripts/run_import_1000yi_once.sh 1000 \
  > logs/run_import_1000yi_once.nohup.log 2>&1 < /dev/null &
echo $!
```

这条命令会自动做完整流程：
- 启动 demo stack
- 先根据 `txt` 生成这 `1` 个模板
- 再基于这个模板导入 `1000` 个 namespace
- 全程在后台运行
- 终端关闭后也继续执行
- 最后自动停掉 demo stack

看进度：
```bash
tail -f logs/run_import_1000yi_once.nohup.log
```

如果模板已经提前建好了，就不用再传 `MASSTREE_PATH_LIST_FILE`，直接：

```bash
nohup env \
  MASSTREE_TEMPLATE_ID=template-pathlist-100m \
  MASSTREE_TEMPLATE_MODE=page_fast \
  bash scripts/run_import_1000yi_once.sh 1000 \
  > logs/run_import_1000yi_once.nohup.log 2>&1 < /dev/null &
echo $!
```