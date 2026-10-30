# L06 — Kết quả của tôi

Máy: laptop 12 luồng CPU, 32 GB RAM, Docker Desktop/WSL2 31 GB · Spark: 4.1.3 local[4], driver 2 GB · Ngày: 2026-10-30

Bảng dưới là **lần chạy thứ 2** (`results/results.csv` chứa cả 2 lần, 34 dòng).
Cột median/max là thời gian chạy task của stage lệch nhất trong query.

| experiment | variant | seconds | total tasks | task median ms | task max ms | plan (final) | note |
|---|---|---|---|---|---|---|---|
| E1 | AQE off, shuffle.partitions=200 | 2.20 | 204 | 6 | 60 | Exchange | |
| E1 | AQE off, shuffle.partitions=8 | 0.33 | 12 | 177 | 178 | Exchange | |
| E1 | AQE off, shuffle.partitions=1 | 0.44 | 5 | 232 | 233 | Exchange | |
| E1 | AQE on, shuffle.partitions=200 | 0.53 | 5 | 271 | 274 | AQEShuffleRead Exchange | AQE gộp các partition shuffle nhỏ |
| E2 | AQE off, broadcast (threshold 10MB) | 1.20 | 13 | 626 | 635 | BroadcastExchange BroadcastHashJoin Exchange | |
| E2 | AQE off, sort-merge (threshold -1) | 1.51 | 21 | 386 | 591 | Exchange SortMergeJoin | |
| E2 | AQE on, threshold -1 + broadcast() hint | 0.40 | 6 | 157 | 168 | AQEShuffleRead BroadcastExchange BroadcastHashJoin Exchange | hint thắng threshold |
| E3 | skewed SMJ, AQE off | 4.10 | 40 | 165 | **2366** | Exchange SortMergeJoin | |
| E3 | salted SMJ (salt=64), AQE off | 4.01 | 56 | 590 | 1197 | Exchange SortMergeJoin | cùng kết quả (500061629) |
| E3 | skewed SMJ, AQE skewJoin on | 2.22 | 27 | 186 | 682 | AQEShuffleRead Exchange SortMergeJoin(skew=true) | 1 partition skew → 4 split |
| E4 | repartition(64) | 7.25 | 68 | 152 | 980 | Exchange | 64 files |
| E4 | repartition(4) | 4.40 | 8 | 2185 | 2210 | Exchange | 4 files |
| E4 | coalesce(4) | 3.12 | 4 | 2907 | 2970 | Coalesce | 3 files |
| E4 | coalesce(1) | 6.95 | 1 | 6800 | 6800 | Coalesce | 1 file |
| E5 | no cache (3 queries) | 6.40 | | | | | 3 aggregations |
| E5 | cache materialisation (count) | 3.42 | | | | | 2,869,714 rows, 59.4 MiB |
| E5 | with cache (3 queries) | 0.99 | | | | | 3 aggregations |

Lần 1 cho cùng xu hướng, chênh khoảng ±20 %:

| | Lần 1 | Lần 2 |
|---|---|---|
| E1-200 | 2.15 s | 2.20 s |
| E3 baseline max | 2686 ms | 2366 ms |
| E3 AQE | 2.28 s | 2.22 s |
| E5 với cache | 0.88 s | 0.99 s |

## Nhận xét

**E1 — shuffle partitions và AQE coalescing.**
- Với 200 partition, query sinh **204 task**, mỗi task chỉ chạy khoảng 6 ms. Chi phí lập lịch và khởi tạo task chiếm hết thời gian, nên chậm nhất (2.2 s, gấp khoảng 6 lần so với 8 partition).
- Bật AQE mà vẫn để 200 partition: node `AQEShuffleRead` gộp các partition nhỏ lại, chỉ còn **5 task** và 0.53 s, gần bằng mức tinh chỉnh tay (8 partition, 0.33 s).
- 1 partition cũng nhanh, nhưng chỉ vì kết quả nhỏ (khoảng 20 nghìn nhóm).

**E2 — broadcast và sort-merge.**
- Threshold -1 buộc Spark dùng `SortMergeJoin`: phải shuffle **cả 3 triệu chuyến** lẫn bảng zone, rồi sort (21 task, max 591 ms).
- Broadcast chỉ gửi 265 dòng tới mọi task, nên không có shuffle phía trips.
- Hint `F.broadcast()` thắng cả threshold -1 và là nhanh nhất (0.40 s, 6 task).
- Ở lần 2, broadcast khi AQE tắt (1.20 s) chỉ nhanh hơn SMJ (1.51 s) một chút vì 8 partition mặc định. Plan vẫn cho thấy khác biệt chính là bỏ được `Exchange` + `Sort` phía bảng lớn.

**E3 — skew: baseline, salting, AQE.**
- 60 % fact (5,998,366 dòng) có key = 0.
- Ở stage join của baseline, **task max 2,366 ms so với median 165 ms (khoảng 14×)**. Task đó đọc **6,264,438 bản ghi** trong khi các task khác chỉ khoảng 262 nghìn (61.9 MiB so với 3.4 MiB, ảnh Stage 42). Đây là một straggler; 3 core còn lại ngồi chờ.
- **Salting** (64 salt, chỉ cho hot key): max giảm còn 1,197 ms và records/task tối đa 917 nghìn. Đổi lại median tăng lên 590 ms vì mỗi partition nhận thêm phần hot key, và thêm cột, rand và 64 bản sao dim. Kết quả: wall-clock gần như không đổi (4.01 s so với 4.10 s), nhưng tổng vẫn đúng (`True`).
- **AQE skew join**: plan có `SortMergeJoin(skew=true)`, và `AQEShuffleRead` báo *number of skewed partitions: 1, skewed partition splits: 4, partitions: 19*. Max còn 682 ms, wall-clock **2.22 s (nhanh gần 2 lần)** mà không phải sửa code.

**E4 — repartition và coalesce.**
- `repartition(64)` chậm nhất (7.25 s): một shuffle đầy đủ cộng 64 file nhỏ.
- `coalesce(4)` không shuffle nên nhanh nhất (3.12 s). Nhưng chỉ ra **3 file** vì file Parquet đầu vào chỉ có 3 row group, nên 1 trong 4 input partition rỗng.
- `coalesce(1)` gom cả scan lẫn ghi vào **1 task** (6.8 s), mất hết song song.
- `repartition(4)` cân bằng (4 file đều nhau) với giá một shuffle.

**E5 — cache.**
- DataFrame "cleaned" có `dropDuplicates`, tức là một shuffle toàn bộ 3 triệu dòng.
- Không cache: 3 aggregation chạy lại dedup 3 lần, tổng 6.40 s.
- Có cache: materialize một lần (3.42 s, 59.4 MiB trong bộ nhớ, `Disk Memory Serialized`), sau đó 3 query chỉ mất **0.99 s**. Tính cả materialize vẫn lợi (4.4 s so với 6.4 s), và lợi nhiều hơn khi tái sử dụng nhiều lần hơn.

## Khuyến nghị cho job production trên cluster 20 node

1. **Luôn bật AQE** (mặc định trong Spark 3.2+/4). Đặt `shuffle.partitions` ban đầu cao (ví dụ bằng 2–3 × tổng số core, khoảng 400–800) và để `coalescePartitions` thu về khoảng 64–128 MB/partition (`advisoryPartitionSizeInBytes`). Không đặt cứng 1 hay 8.
2. **Broadcast bảng dimension nhỏ.** Tăng `autoBroadcastJoinThreshold` lên khoảng 50–100 MB nếu executor đủ bộ nhớ, và dùng hint `broadcast()` khi thống kê sai. Không broadcast bảng hàng trăm MB, vì driver phải collect trước nên dễ OOM.
3. **Skew.** Để AQE skew join xử lý với ngưỡng mặc định (5×, 256 MB). Với hot key đã biết, hoặc skew trong `groupBy`/outer join mà AQE không xử lý, dùng **salting** hoặc tách hot key ra xử lý riêng. Theo dõi chỉ số task max/median trong UI.
4. **Ghi output.** Dùng `repartition(n)` hoặc `repartition(col)` để mỗi file khoảng 128 MB–1 GB và không sinh hàng nghìn file nhỏ. Tránh `coalesce(1)` trên dữ liệu lớn vì nó kéo cả stage trước về 1 task.
5. **Cache có chọn lọc.** Chỉ `persist(MEMORY_AND_DISK)` kết quả **đắt** và được **dùng lại** ít nhất 2 lần, rồi `unpersist()` ngay. Không cache scan Parquet rẻ.
