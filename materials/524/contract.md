# 树与对象输入

tree.json 的 commit 是本题固定树身份，不要求启动 Git。entries 每项 path 是目标相对路径，filter 为 lfs/none，blob 是材料目录内的只读 Git blob 文件相对路径，blob_sha256 绑定其字节。objects/<oid> 是下载设施可返回的原始正文；两个 LFS 路径共享一个对象。相同 pointer 文本的普通文件必须原样保留。source_fixture.Objects 是最小对象设施，不是目标缓存/检出实现；候选可扩展本地传输测试设施以注入公开故障，但不能让错误内容通过长度与摘要检查。
