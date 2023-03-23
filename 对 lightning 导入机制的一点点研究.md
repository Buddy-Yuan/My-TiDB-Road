### 前言

最近生产上出现了一个问题，就是一堆 `empty region` 不进行合并。通过分析发现是和`lightning`失败相关的，于是把这个问题研究了一下，以下是关于这个问题的一点点原理。

### Lightning 究竟停止了什么

首先我们先阅读一下官方文档。

> 在导入数据之前，tidb-lightning 会自动将 TiKV 节点切换为“导入模式” (import mode)，优化写入效率并停止自动压缩。tidb-lightning 会根据 TiDB 集群的版本决定是否停止全局调度。
> 
> 1. 当 TiDB 集群版本 >= v6.1.0 且 TiDB Lightning 版本 >= v6.2.0 时，`tidb-lightning` 在向 TiKV 导入数据时，只会暂停目标表数据范围所在 region 的调度，并在目标表导入完成后恢复调度。
> 2. 当 TiDB 集群版本 < v6.1.0 或 TiDB Lightning 版本 < v6.2.0 时，`tidb-lightning` 会暂停全局调度。

通过官方文档的导读，我们可以清楚的知道，当TiDB版本<6.1版本的时候 或者是 TiDB Lightning<6.2.0的时候，它会暂停全局调度。这也是我本次遇到的问题，它究竟停了哪些调度？

### 做个实验验证一下

要简单验证这个问题，就是找个 `TiDB Lightning` 版本 <6.2 的验证一下，我这里是 6.1 的版本，我做了一个简单的 `lightning` 的导入。在日志中我们发现了以下日志。

```go
[2023/03/11 14:16:55.211 +08:00] [INFO] [pd.go:416] ["pause scheduler successful at beginning"] [name="[balance-hot-region-scheduler,balance-leader-scheduler,balance-region-scheduler]"]
[2023/03/11 14:16:55.225 +08:00] [INFO] [pd.go:424] ["pause configs successful at beginning"] [cfg="{\\"enable-location-replacement\\":\\"false\\",\\"leader-schedule-limit\\":12,\\"max-merge-region-keys\\":0,\\"max-merge-region-size\\":0,\\"max-pending-peer-count\\":2147483647,\\"max-snapshot-count\\":40,\\"region-schedule-limit\\":40}"]
```

打开6.1的源码，搜索一下`pause scheduler successful at beginning`关键字，发现是在 `br -> pkg -> pdutl - > pd.go` 代码中，和我们日志打印的信息可以辉映上。

```go
func (p *PdController) pauseSchedulersAndConfigWith(ctx context.Context, schedulers []string, schedulerCfg map[string]interface{}, post pdHTTPRequest, ) ([]string, error) {
   // first pause this scheduler, if the first time failed. we should return the error
   // so put first time out of for loop. and in for loop we could ignore other failed pause.
   removedSchedulers, err := p.doPauseSchedulers(ctx, schedulers, post)
   if err != nil {
      log.Error("failed to pause scheduler at beginning",
         zap.Strings("name", schedulers), zap.Error(err))
      return nil, errors.Trace(err)
   }
   log.Info("pause scheduler successful at beginning", zap.Strings("name", schedulers))
   if schedulerCfg != nil {
      err = p.doPauseConfigs(ctx, schedulerCfg, post)
      if err != nil {
         log.Error("failed to pause config at beginning",
            zap.Any("cfg", schedulerCfg), zap.Error(err))
         return nil, errors.Trace(err)
      }
      log.Info("pause configs successful at beginning", zap.Any("cfg", schedulerCfg))
   }

   go func() {
      tick := time.NewTicker(pauseTimeout / 3)
      defer tick.Stop()

      for {
         select {
         case <-ctx.Done():
            return
         case <-tick.C:
            _, err := p.doPauseSchedulers(ctx, schedulers, post)
            if err != nil {
               log.Warn("pause scheduler failed, ignore it and wait next time pause", zap.Error(err))
            }
            if schedulerCfg != nil {
               err = p.doPauseConfigs(ctx, schedulerCfg, post)
               if err != nil {
                  log.Warn("pause configs failed, ignore it and wait next time pause", zap.Error(err))
               }
            }
            log.Info("pause scheduler(configs)", zap.Strings("name", removedSchedulers),
               zap.Any("cfg", schedulerCfg))
         case <-p.schedulerPauseCh:
            log.Info("exit pause scheduler and configs successful")
            return
         }
      }
   }()
   return removedSchedulers, nil
}

```

这段代码调用 `p.doPauseSchedulers`，`p.doPauseConfigs` 两个函数，分别去暂停 `Scheduler` 和 `Config`。然后这个方法里面还有一个 `go func() {…}()`，它会创建和启动一个协程，这个协程会循环根据上下文信息，来判断是否退出循环和结束协程。这个协程里面执行的操作也是暂停 `Scheduler` 和 `Config`。这样做的目的是保证 `lightning` 在执行的过程中，`Scheduler` 和 `Config` 必须处于暂停或者关闭的状态，如果被谁中途打开了，就继续再关闭它。

> 需要注意的一点是这个函数的两个入参，分别是 `schedulers`、`schedulerCfg`，这两个参数分别被传入给了 `doPauseSchedulers` 和 `doPauseConfigs` 函数，也就是暂停 `Scheduler` 和 `Config` 的函数，因此，我们分析停止了什么，就要分析函数的入参（ `schedulers`、`schedulerCfg`）
> 

而这两个入参，稍微翻一下代码就可以找到。`schedulers`  上面有定义。

```go
	Schedulers = map[string]struct{}{
		"balance-leader-scheduler":     {},
		"balance-hot-region-scheduler": {},
		"balance-region-scheduler":     {},

		"shuffle-leader-scheduler":     {},
		"shuffle-region-scheduler":     {},
		"shuffle-hot-region-scheduler": {},
	}
```

这里就是要停止的调度。这里列了6种，而其实对我们来说它只会停止前面三种。因为当前数据库默认装好是以下4种调度。

```go
[root@test ~]# tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 scheduler show
Starting component `ctl`: /root/.tiup/components/ctl/v6.1.1/ctl pd -u 127.0.0.1:2379 scheduler show
[
  "balance-hot-region-scheduler",
  "balance-leader-scheduler",
  "balance-region-scheduler",
  "split-bucket-scheduler"
]
```

`schedulerCfg` 这个入参稍微复杂一些，我们需要找到函数上一层的调用。

```go
func (p *PdController) doRemoveSchedulersWith(
	ctx context.Context,
	needRemoveSchedulers []string,
	disablePDCfg map[string]interface{},
) ([]string, error) {
	var removedSchedulers []string
	var err error
	if p.isPauseConfigEnabled() {
		// after 4.0.8 we can set these config with TTL
		removedSchedulers, err = p.pauseSchedulersAndConfigWith(ctx, needRemoveSchedulers, disablePDCfg, pdRequest)
	} else {
		// adapt to earlier version (before 4.0.8) of pd cluster
		// which doesn't have temporary config setting.
		err = p.doUpdatePDScheduleConfig(ctx, disablePDCfg, pdRequest)
		if err != nil {
			return nil, err
		}
		removedSchedulers, err = p.pauseSchedulersAndConfigWith(ctx, needRemoveSchedulers, nil, pdRequest)
	}
	return removedSchedulers, err
}
```

上层函数调用的时候，看注释其实分两种情况，一种是4.0.8之后，注意看这里调用 `pauseSchedulersAndConfigWith` 的时候传入的参数是 `disablePDCfg`。所以只要弄清楚 `disablePDCfg` 是什么就清楚了它要停的 `config` 配置。而 `disablePDCfg` 信息继续搜索得知，它的取值是循环自 `expectPDCfg` 的。

```go
disablePDCfg := make(map[string]interface{}, len(expectPDCfg))
	originPDCfg := make(map[string]interface{}, len(expectPDCfg))
	for cfgKey, cfgValFunc := range expectPDCfg {
		value, ok := scheduleCfg[cfgKey]
		if !ok {
			// Ignore non-exist config.
			continue
		}
		disablePDCfg[cfgKey] = cfgValFunc(len(stores), value)
		originPDCfg[cfgKey] = value
	}
```

那么 `expectPDCfg`  又是什么呢 ？其实在最上面也定义了。

```go
expectPDCfg = map[string]pauseConfigGenerator{
		"max-merge-region-keys": zeroPauseConfig,
		"max-merge-region-size": zeroPauseConfig,
		// TODO "leader-schedule-limit" and "region-schedule-limit" don't support ttl for now,
		// but we still need set these config for compatible with old version.
		// we need wait for https://github.com/tikv/pd/pull/3131 merged.
		// see details https://github.com/pingcap/br/pull/592#discussion_r522684325
		"leader-schedule-limit":       pauseConfigMulStores,
		"region-schedule-limit":       pauseConfigMulStores,
		"max-snapshot-count":          pauseConfigMulStores,
		"enable-location-replacement": pauseConfigFalse,
		"max-pending-peer-count":      constConfigGeneratorBuilder(maxPendingPeerUnlimited),
	}
```

到这里，我们基本清楚了它要停止的7个config配置信息。但是它要改成多少呢 ？从上面的代码可以得知为下面的值。

```go
"max-merge-region-keys" -> 0
"max-merge-region-size" -> 0
"leader-schedule-limit" -> (函数返回值是return math.Min(40, rawCfg*float64(stores))，返回值是将当前值 * store数量，然后和40比较那个值小选哪个)
"region-schedule-limit" -> (函数返回值是return math.Min(40, rawCfg*float64(stores))，返回值是将当前值 * store数量，然后和40比较那个值小选哪个)
"max-snapshot-count"    -> (函数返回值是return math.Min(40, rawCfg*float64(stores))，返回值是将当前值 * store数量，然后和40比较那个值小选哪个)
"enable-location-replacement" -> 修改成pauseConfigFalse ，函数返回值是false
"max-pending-peer-count" -> 修改成 constConfigGeneratorBuilder(maxPendingPeerUnlimited)，maxPendingPeerUnlimited uint64 = math.MaxInt32，这个值是2的31次方=2147483648
```

到此为止我们就彻底从源码上弄清楚了，在< 6.2 版本的 lightning 中，究竟会停止哪些schedule 和 config 。

### 总结一下如何恢复

最后总结一下如何恢复，当 lightning 出现异常后，根据我前面查看的配置反向操作就行了。

1. 首先需要检查 `schedulers` 的状态,如果发现 `schedulers` 处于暂停状态，需要按照下一步来恢复。

```go
//查看当前暂停的调度
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 scheduler show --status paused

//将以下停止的调度 resume
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 scheduler resume "balance-hot-region-scheduler"
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 scheduler resume "balance-region-scheduler"
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 scheduler resume "split-bucket-scheduler"
```

1. 恢复config 的值。

```go
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set max-merge-region-keys 200000
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set max-merge-region-size 20
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set leader-schedule-limit 4
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set region-schedule-limit 2048
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set enable-location-replacement true
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set max-pending-peer-count 64
tiup ctl:v6.1.1 pd -u 127.0.0.1:2379 config set max-snapshot-count 64
```

1. 恢复完成后，可以到 grafana 相关面板里面去检查一下调度是否在正常运行。

> 需要注意的是，以上步骤仅适用于 `tidb-lightning` 版本 < 6.2 的情况。
>
