# 轮次事件与服务

样例把OData事件规范化为以下JSON，不宣称覆盖完整OData协议：

- entity：`{type:"entity",id,name?,note?}`。id相当于@id；新增id必须有非null name，note省略则为null；已存id省略业务字段保留。
- entityDelete：`{type:"entityDelete",id}`，对应@removed，删除不存在实体幂等。
- linkAdd/linkDelete：`{type,source,nav,target}`，nav是固定关系名child/owner，关系是集合，不允许重复数。

事件未知字段非法，所有身份是非空文本，name为非空文本，note是文本或null。round_id/页序和nextLink/deltaLink位于页对象顶层。服务提供的页面在同round身份下不可变；测试可通过重配设施提供协议违约，候选必须拒绝。

初始请求为`/account/delta/t1`。服务根在本机，所有可跟随链接必须位于相同scheme/host/port且路径归一化后在`/account/`内；不因URL字面包含该前缀就放行。URL可以是同根相对地址，不能把URL正文作为任意本机文件路径。

采集服务要保存页证据，而不要求特定HTTP框架；Pages是可调用本地源设施，候选可封成HTTP进程或直接对接该接口。轮次中的事件顺序是页序与页内顺序的串联。
