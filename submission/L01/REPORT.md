# L01 — HDFS: blocks, replicas và sự cố DataNode

Môi trường: WSL2 (x86_64, 12 CPU, Docker 31.9 GB), Hadoop 3.4.3 — 1 NameNode, 3 DataNode, RM, NM, JobHistory.
Log gốc của mọi bước nằm trong các file `0*.txt` ở thư mục này; ảnh ở `img/`.

Ánh xạ IP → hostname (từ `hdfs dfsadmin -report`):

| IP | Host |
|---|---|
| 172.29.0.5 | datanode1 |
| 172.29.0.2 | datanode2 |
| 172.29.0.3 | datanode3 |

## 1–2. Khởi động cluster và `hdfs dfs` shell

![cluster + dfs shell](img/01_cluster_and_dfs_shell.png)
![NameNode overview](img/11_ui_namenode_overview_healthy.png)
![Datanodes](img/12_ui_datanodes_healthy.png)

**TODO 2.1**
```bash
hdfs dfs -stat "name=%n blocksize=%o repl=%r size=%b" /data/taxi/taxi_zone_lookup.csv
# name=taxi_zone_lookup.csv blocksize=134217728 repl=3 size=12331
```

## 3. Block mặc định (128 MB) và block 16 MB

| Upload | Số block | Kích thước block |
|---|---|---|
| mặc định 128 MB | 1 | 49,961,641 |
| `-D dfs.blocksize=16m` | 3 | 16,777,216 + 16,777,216 + 16,407,209 |

![fsck 128m vs 16m](img/02_blocks_128m_vs_16m.png)

Các file block vật lý trên datanode1 (`blk_<id>` + `blk_<id>_<genstamp>.meta`); block cuối chỉ 16,407,209 byte, không bị pad lên 16 MB:

![block files](img/03_physical_block_files_datanode1.png)

## 4. Replication factor

`setrep -w 2` → `Live_repl=2` cho cả 3 block; dung lượng raw của `small_blocks` giảm từ 142.9 M xuống **95.3 M**.

**TODO 4.1**
```bash
hdfs dfs -D dfs.replication=1 -put /datasets/data/taxi/taxi_zone_lookup.csv /tmp/one_replica.csv
# one_replica.csv repl=1   (replica duy nhất nằm trên 172.29.0.5 = datanode1)
```

![setrep](img/04_setrep2_and_todo4.1.png)

## 5. Tắt DataNode → under-replication → re-replication

Tắt **datanode3** (đang giữ replica của cả 3 block của file `small_blocks`).

**(a) `fsck` của file block 16 MB TRƯỚC khi tắt** (replica nằm trên `.5` datanode1 và `.3` datanode3):

![before](img/05_fsck_small_blocks_BEFORE_kill.png)

**(b) Thời gian đến khi node bị coi là chết:** **90 s** (`Dead datanodes (1)` lúc t+90 s). Lúc t+94 s
(10:17:40) NameNode log `removeDeadDatanode: lost heartbeat from 172.29.0.3`; từ lần poll t+98 s trở đi
`Under replicated blocks: 2`. Con số 90 s khớp với lý thuyết 2 × recheck-interval (30 s) + 10 × heartbeat (3 s).

![timeline](img/06_kill_datanode3_timeline.png)
![UI overview dead](img/14_ui_overview_datanode3_DEAD.png)
![UI datanodes dead](img/15_ui_datanodes_datanode3_DEAD.png)

**(c) `fsck` của file block 16 MB SAU khi tắt:** cả 3 block vẫn `Live_repl=2`, nhưng replica trên `.3` đã được
**chép lại sang `.2` (datanode2)** → file trở lại `OK`.

![after](img/07_fsck_small_blocks_AFTER_kill.png)

Hai file replication 3 (`default_blocks/...parquet`, `taxi_zone_lookup.csv`) vẫn **under-replicated** (2 block, 40 %)
nhưng `Status: HEALTHY`; md5 của file tải về trùng với bản gốc → không mất dữ liệu.
(`/tmp/one_replica.csv` nằm trên datanode1 nên không bị MISSING.)

![under-replicated + md5](img/08_fsck_data_underreplicated_md5.png)

Bật lại datanode3 → `Under-replicated 0`, `Over-replicated 0` (replica thừa đã bị xoá):

![back](img/09_datanode3_back.png)

## 6–7. Safe mode và fsimage

![safemode](img/10_safemode_fsimage.png)

Trong safe mode lệnh ghi bị từ chối (`Name node is in safe mode`), lệnh đọc vẫn chạy. Sau `restart namenode`
safe mode tự bật vài giây. fsimage (`hdfs oiv`) chứa tên file/thư mục và block ID nhưng **không có vị trí block**
(grep thông tin DataNode/port 9866 trả về 0 dòng).

## Trả lời checkpoint

**Q1. File 300 MB, block mặc định, replication 3?**
⌈300/128⌉ = **3 block** (128 + 128 + 44 MB) → **9 block replica** → ~**900 MB** đĩa raw (cộng thêm file `.meta`
checksum rất nhỏ). NameNode chỉ giữ 3 đối tượng block trong RAM, không phụ thuộc replication.

**Q2. Vì sao block cuối nhỏ hơn 16 MB, có lãng phí 16 MB không?**
File kết thúc ở đó: 49,961,641 − 2 × 16,777,216 = 16,407,209 byte. Block size chỉ là kích thước **tối đa**;
file block trên DataNode đúng bằng lượng dữ liệu thực (thấy rõ ở ảnh 03), nên không lãng phí đĩa.

**Q3. Danh sách vị trí block được lưu ở đâu? Restart NameNode thì sao?**
Chỉ nằm trong **RAM của NameNode**, không có trong fsimage hay edit log (ảnh 10). Khi restart, NameNode dựng lại
block map từ **block report** của các DataNode, vì thế nó phải nằm trong safe mode đến khi nhận đủ report.

**Q4. Vì sao file replication 2 được chữa còn file replication 3 thì không?**
Re-replication cần một DataNode **chưa có** replica của block đó. Khi chỉ còn 2 node sống: block rep-2 mất một
bản trên datanode3 → chép được sang datanode2 (node còn trống). Block rep-3 thì đã có bản trên **cả hai** node còn
lại, không còn node thứ ba để đặt bản sao, và HDFS không bao giờ đặt 2 replica của cùng một block trên một DataNode.
Vì vậy nó vẫn under-replicated cho đến khi datanode3 quay lại.

**Q5. `fsck` báo gì khi under-replicated, vì sao không phải lỗi?**
`Status: HEALTHY` kèm `Under-replicated blocks: 2 (40.0 %)`. HEALTHY nghĩa là block nào cũng có ít nhất
`dfs.namenode.replication.min` (=1) replica sống, nên toàn bộ dữ liệu vẫn đọc được (md5 khớp). Under-replication chỉ
là **rủi ro** (giảm độ dư thừa); lỗi thật là MISSING/CORRUPT.

**Q6. Với `recheck-interval` mặc định (5 phút) thì sao? Vì sao timeout dài là mặc định tốt?**
Node bị coi là chết sau 2 × 5 phút + 10 × 3 s = **10.5 phút** (lab: 90 s). Timeout dài tránh **replication storm**:
một node reboot hay chập mạng ngắn sẽ khiến cluster chép hàng TB block, rồi các bản đó lại thành thừa khi node quay lại
sau một phút, tốn băng thông và I/O. Trong lúc chờ, client vẫn tránh các node *stale* (30 s không có heartbeat) khi đọc/ghi.

**Q7. Vì sao NameNode khởi động trong safe mode, vì sao phải chặn ghi?**
Lúc khởi động NameNode biết namespace (fsimage + edits) nhưng chưa biết block nằm ở đâu. Trước khi ≥ 99.9 % block
được báo cáo, nó không phân biệt được block "bị mất" với block "chưa báo cáo". Nếu cho phép ghi/xoá hoặc bắt đầu
re-replication thì nó sẽ quyết định sai, ví dụ lên lịch chép hàng loạt các block chỉ đơn giản là chưa được báo cáo.
Safe mode = chỉ đọc cho tới khi block map đáng tin cậy.

**Q8. 1 file 1 KB và 1 triệu file 1 KB: vì sao trường hợp sau là vấn đề?**
Mỗi file, thư mục và block là một object trong heap NameNode (~150 B). 1 triệu file nhỏ = ≥ 2 triệu object ≈ 300 MB
heap cho vỏn vẹn 1 GB dữ liệu, cộng thêm 1 triệu RPC và sau này 1 triệu map task (mỗi file một split). Đây là
**small files problem**. Cách xử lý: gộp file, dùng định dạng container (Parquet/ORC, SequenceFile, HAR).
