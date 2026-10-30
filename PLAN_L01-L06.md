# Kế hoạch hoàn thành L01–L06 (data_engineering_labs)

Repo: `data_engineering_labs/` (clone từ HuynhHoangPhuc/data_engineering_labs).
Đầu ra cần nộp lấy từ mục **Grading hints** trong `INSTRUCTOR_NOTES.md` của từng lab.

## Bước 0 — Chuẩn bị (bắt buộc)

- [ ] **Docker chưa dùng được trong WSL** → bật Docker Desktop → Settings → Resources → *WSL integration* cho distro này (hoặc cài Docker Engine trong WSL). Cấp Docker ≥ 4 GB RAM (máy có 31 GB, nên cấp 8 GB).
- [ ] `bash datasets/download_taxi.sh` (taxi 2024-01 + zone lookup, ~48 MB)
- [ ] `bash L00_setup/check_env.sh` → phải in `READY`
- [ ] Lưu ý: README giả định repo nằm ở `labs/`; ở đây là `data_engineering_labs/`, chỉ cần `cd` cho đúng.
- [ ] Tạo thư mục `submission/` để gom output (log, bảng, câu trả lời) cho từng lab.

Nguyên tắc: **mỗi lúc chỉ chạy 1 stack**, `docker compose down` trước khi sang lab khác (L01→L02 thì dùng chung cluster).

---

## L01 — HDFS (90–120')

**Việc cần làm:** dựng cluster 3 DataNode → `hdfs dfs` → upload taxi với block 128 MB và 16 MB → `fsck` → `setrep 2` → kill `datanode3` → quan sát under-replication / re-replication → safe mode.
TODO: 2.1 (`hdfs dfs -stat "%n %o %r %b"`), 4.1 (`hdfs dfs -D dfs.replication=1 -put ...`).

**Đầu ra phải nộp:**
1. Output `hdfs fsck /data/taxi/small_blocks -files -blocks -locations` **trước và sau** khi kill DataNode.
2. Thời gian đến khi node bị tuyên bố dead (~90 s).
3. Trả lời checkpoint Q1, Q4, Q6, Q7 (điểm tối đa: giải thích *vì sao* file rep-3 vẫn under-replicated, *vì sao* block locations không được lưu xuống đĩa – Q3/Q7).

## L02 — MapReduce Streaming (90–120', dùng lại cluster L01)

**Việc cần làm:** TODO 1–3 (wordcount mapper/reducer), TODO 4–6 (`zones/mapper.py`: map-side join, đếm BAD_RECORDS) → test bằng pipe → chạy trên YARN → chạy zones job **có và không có combiner**.

**Đầu ra phải nộp:**
1. File `src/wordcount/mapper.py`, `reducer.py`, `src/zones/mapper.py` đã hoàn thiện.
2. **Bảng combiner** (Launched map tasks, Map output records, Combine output records, Map output materialized bytes, Reduce shuffle bytes, Reduce input records, wall-clock).
3. Trả lời Q3–Q6 (Q6 phải có ý tưởng emit `(sum, count)`).
4. Bonus: stretch "average fare per zone" với combiner đúng.

Kết quả đúng để tự kiểm: top word `the 25806`; top zone `Queens/JFK Airport 59777`.

## L03 — Hive (2h)

**Việc cần làm:** convert Parquet→CSV lên HDFS; TODO 1 (`zones` với OpenCSVSerde), TODO 2–4 (nonstrict dynamic partition, `pickup_month`, bảng Parquet), TODO 5 (pruning), TODO 6 (bucketing 4 buckets).

**Đầu ra phải nộp:**
1. Các file `sql/01…05_*.sql` đã hoàn thiện.
2. **Bảng dung lượng** CSV / ORC / Parquet (`hdfs dfs -du -h`).
3. **Bảng thời gian** Q1 trên CSV / ORC / Parquet (lấy lần chạy thứ 2).
4. Trả lời Q1, Q3, Q4, Q6, Q7 (Q4 phải nhắc *column pruning* **và** *partition pruning*; Q6 phải nhắc small files / áp lực metastore).

Tự kiểm: `csv_rows = 2,964,624`; partitions `2002-12, 2009-01, 2023-12, 2024-01`; bucketed table có đúng 4 file.

## L04 — Ingestion: Sqoop → Spark JDBC (2–2.5h)

**Việc cần làm:** Part 1 Sqoop (full import, TODO 1.2 customers split theo zip, incremental append, saved job, lastmodified + merge, export). Part 2 `src/spark_jdbc_ingest.py` TODO 1–3 (đọc song song 4 partition, ghi Parquet theo tháng) và TODO 4–5 (watermark + dedup `row_number`). Part 3 bảng so sánh.

**Đầu ra phải nộp:**
1. **Bảng so sánh Sqoop vs Spark JDBC** (kèm thời gian full import của chính mình).
2. Số dòng mỗi part-file: `orders` (1250×4) vs `customers` (≈2450/1146/427/977) và giải thích.
3. File `spark_jdbc_ingest.py` đã hoàn thiện.
4. Trả lời Q1, Q3, Q4, Q6 (Q6 phải nhắc **DELETE** và **các update trung gian giữa 2 lần chạy** / transaction commit muộn).

## L05 — PySpark basics (2h)

**Việc cần làm:** `src/l05_basics.py` TODO 1–7 (đếm NULL, luật làm sạch, join 2 lần với zones, aggregate theo borough, window `dense_rank`/`lag`/running sum, ghi Parquet partition theo `pickup_date`). Xem Spark UI với `--hold 600`.

**Đầu ra phải nộp:**
1. File `src/l05_basics.py` đã hoàn thiện.
2. Trả lời checkpoint (trọng tâm Q3: lazy evaluation/DAG; Q5: đọc formatted plan — Exchange = shuffle, partial/final HashAggregate, BroadcastHashJoin).

Tự kiểm: kept `2,867,786` (96.73 %); Manhattan `2,571,810` trips; ghi ra 31 file; filter 2024-01-15 → `74,743`.

## L06 — Spark tuning (2h)

**Việc cần làm:** `src/l06_experiments.py` TODO 1 (AQE on + 200 partitions), TODO 2 (`F.broadcast` hint), TODO 3 (salting hot key, salt 0..63), TODO 4 (AQE skew join với threshold hạ thấp). Chạy toàn bộ **2 lần**, lấy lần 2.

**Đầu ra phải nộp:**
1. `results/results.csv`.
2. `results/RESULTS.md` đã điền: bảng kết quả + nhận xét E1–E5 + khuyến nghị cho cluster 20 node.
   Điểm tối đa: phân tích skew (max ≫ median task time), gọi tên node AQE thấy được (`AQEShuffleRead`, `BroadcastHashJoin` sau khi demote), khuyến nghị có lý do cho từng experiment.

---

## Thứ tự & ước lượng

| # | Lab | Thời gian | Phụ thuộc |
|---|---|---|---|
| 0 | Setup Docker + dataset | 30–60' (build image hadoop lần đầu 3–6') | — |
| 1 | L01 | ~2h | Bước 0 |
| 2 | L02 | ~2h | cluster L01 còn chạy |
| 3 | L03 | ~2h | image Hive ~4 GB, pull trước |
| 4 | L04 | ~2.5h | image sqoop (build từ hadoop) |
| 5 | L05 | ~2h | image spark |
| 6 | L06 | ~2h | L05 |

Tổng ≈ 12–13h. Có thể code trước các TODO của L02/L05/L06 (đối chiếu `solutions/`) rồi chạy lấy số liệu sau.

## Cấu trúc thư mục nộp đề xuất

```
submission/
  L01/ fsck_before.txt fsck_after.txt answers.md
  L02/ mapper/reducer *.py combiner_table.md answers.md
  L03/ sql/*.sql size_table.md timing_table.md answers.md
  L04/ spark_jdbc_ingest.py compare_table.md part_counts.txt answers.md
  L05/ l05_basics.py answers.md
  L06/ results.csv RESULTS.md
```
