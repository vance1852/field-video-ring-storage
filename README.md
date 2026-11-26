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

## 循环存储服务实现

`ring_service.py` 是仅依赖标准库的核心服务；`ring_app.py` 提供 HTTP/JSON、原始字节和 fixture 回放入口。`solution.py`、`server.py`、`app.py`、`service.py`、`ring.py` 与 `__main__.py` 是兼容入口。

### 本地回放与检查

~~~sh
# 回放两个现场资料；默认把 ring.img 和元数据写入临时或指定目录
python3 solution.py fixtures/wrapped-write-while-two-holds-overlap \
  --state-dir /tmp/ring-wrapped

python3 solution.py fixtures/no-unused-space-until-release \
  --state-dir /tmp/ring-no-space

# 只使用内存盘
python3 -m ring_app simulate fixtures/wrapped-write-while-two-holds-overlap --memory

# 全量测试
python3 -m unittest -v
~~~

回放会核对绕卷 extent、部分写/断连、重启恢复、留段重叠、空间承诺/拒绝、实际 SHA-256 字节、缺口和连续序号水位。

### HTTP 服务

~~~sh
RING_PORT=8080 python3 solution.py serve --fixture fixtures/wrapped-write-while-two-holds-overlap
# 或不带 fixture：
python3 solution.py serve --port 8080
~~~

常用接口：

* `PUT /ring/{offset}`：原始连续字节，不允许单次写跨介质右边界。
* `GET /ring/{offset}?length=N`：读取实际字节。
* `POST /event`：提交资料中的事件对象；若服务由 `--fixture` 启动，会自动读取 `file` 对应对象字节。
* `POST /capture`：片段写入，JSON 字节放在 `data_b64`；也可用 `application/octet-stream` 加查询参数。
* `POST /open`、`POST /close`、`POST /retain`、`POST /collect`、`POST /query`。
* `POST /power_loss`、`POST /restart`、`GET /status`、`GET /usage`、`GET /progress`。

### Python API

~~~python
from ring_service import LoopRingService

svc = LoopRingService(
    capacity=24,
    state_dir="/tmp/ring",       # 不传则使用内存盘
    next_offset=20,
    clock_bounds={"p/1": [0, 0], "p/2": [4, 6]},
    rate_bound={"p": 2},
)
svc.add_initial_segment(...)
svc.open(token="v1", camera="p", span=[8, 11], until=23, at=14)
svc.create_hold("h1", "p", [11, 18], until=31, at=15)
svc.submit_capture(camera="p", boot=1, seq=9, clock=14, duration=3,
                   at=17, data=b"...")
svc.handover("h1", at=24, include_data=True)
svc.power_loss()
svc.restart()
~~~

片段身份为 `(camera, boot, seq)`；不同 boot 即使序号相同也不会合并。相机时间区间为 `[clock, clock+duration)`；若对时偏移为 `[lo, hi]`，本地可能区间是 `[clock+lo, clock+duration+hi)`，所有偏移下都确认覆盖的核心区间是 `[clock+hi, clock+duration+lo)`，边界不唯一时标记 `uncertain`。
