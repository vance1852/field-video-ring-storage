# 现场视频循环卷的留段与写入反压

存储联调使用合成设备、时间轴和原始字节。设备记录由各客户端及介质的输入组成，每份记录可独立使用。

## 业务协议

环卷的 `capacity` 单位为字节，介质位置可在尾部折返。 `capture` 同时包含相机、`boot`、`seq`、相机时间 `clock`、到达时刻 `at`、片段时长与字节来源。 `clock_bounds` 的值是本次启动期的相机时间加到本地时间的最小和最大偏移，无法归属的片段保留不确定性。 `rate_bound` 给各相机每个时间单位能够承诺的最大字节数，以便确知未到达期间的用量上界。
`PUT /ring/{offset}` 接受从该介质位置起的连续原始字节，单次写不跨介质右边界。 `stored` 表示整次设备写持久化，`partial` 的 `written` 数值仅证明开头写了所示字节数。区间均按本地时间轴左闭右开，留段 `until` 是领取截止。 `initial` 给初始已知介质段的物理位置和时间证据，`next_offset` 是开始演练前的写位置。

## 本地设备资料

运行环境为 Python 3.11.2，使用标准库。设备资料端默认仅在 127.0.0.1:8090 运行，使用 --port 可另设端口。

~~~sh
python3 device_fixture.py fixtures/wrapped-write-while-two-holds-overlap
~~~

GET /fixture/events 返回设备参数、字节目录和按到达顺序记录的输入。同一时刻按文件先后排列。GET /fixture/objects/文件名 取原始字节。GET /fixture/journal 查看资料端已收到的设备请求，每个请求记录实际正文长度和摘要。

events.json 中的 packets 是可重放的设备响应。资料端按实际 HTTP 方法、路径和完整原始请求正文匹配第一条尚未使用的记录。没有匹配时返回 404。lag_ms 是回包延迟，drop 代表设备收到请求后直接断开连接，接收方需要以其他报告确认结果。没有 HTTP 交互的设备和客户端动作直接取 events。重新启动资料端会回到记录开头。

在项目根目录运行 python3 -m unittest -v 检查事件、对应字节、设备响应及接收日志。
