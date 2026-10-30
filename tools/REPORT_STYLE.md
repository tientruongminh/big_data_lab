# Quy ước viết báo cáo thực hành (L01–L06)

Mục tiêu: mỗi `submission/LXX/REPORT.md` đọc như một **báo cáo học thuật** (kiểu bài báo/báo cáo kỹ thuật),
viết bằng **tiếng Việt trang trọng**. File được chuyển sang LaTeX/PDF bằng pandoc + pandoc-crossref, nên phải theo
đúng cú pháp dưới đây.

## Cấu trúc bắt buộc

```markdown
# <Tiêu đề lab, ví dụ: HDFS: tổ chức block, nhân bản và khả năng chịu lỗi DataNode> {#sec:l01}

## Tóm tắt {.unnumbered}
Một đoạn 120–200 từ: mục tiêu, phương pháp, kết quả định lượng chính, kết luận.

**Từ khóa:** HDFS, replication, ...

## Giới thiệu {#sec:l01-intro}
Bối cảnh, vấn đề, mục tiêu cụ thể (danh sách đánh số), cấu trúc phần còn lại của báo cáo.

## Cơ sở lý thuyết {#sec:l01-theory}
Các khái niệm cần thiết, ngắn gọn (3–6 tiểu mục ###), có trích dẫn [1], [2].

## Môi trường và phương pháp thực nghiệm {#sec:l01-method}
### Môi trường        (phần cứng, phiên bản phần mềm, cấu hình cluster: nên là bảng)
### Dữ liệu           (nguồn, kích thước, số dòng)
### Quy trình         (các bước thực nghiệm, đánh số; nêu tham số đã thay đổi)

## Kết quả thực nghiệm {#sec:l01-results}
Các tiểu mục ### theo từng thí nghiệm. Mỗi tiểu mục: mô tả ngắn → bảng/hình → nhận xét khách quan về số liệu.

## Thảo luận {#sec:l01-discussion}
Trả lời **toàn bộ** câu hỏi checkpoint, mỗi câu một tiểu mục:
### Câu hỏi 1: <diễn đạt lại câu hỏi> {#sec:l01-q1}
Lập luận dựa trên số liệu ở phần Kết quả (tham chiếu @fig:..., @tbl:...).
Có thể thêm tiểu mục "Hạn chế của thực nghiệm" (local mode, một máy, dữ liệu nhỏ, v.v.).

## Kết luận {#sec:l01-conclusion}
Tóm lược phát hiện chính + khuyến nghị thực tiễn + hướng mở rộng.

## Tài liệu tham khảo {.unnumbered}
[1] Tác giả, "Tên bài/tài liệu," Nơi xuất bản, năm. URL nếu có.
(5–8 mục, kiểu IEEE, chỉ dùng tài liệu có thật: tài liệu Apache chính thức, bài báo gốc GFS 2003, MapReduce 2004,
Hive VLDB 2009, Spark NSDI 2012, Spark SQL SIGMOD 2015, Sqoop docs, Tom White "Hadoop: The Definitive Guide" 4th ed.,
"Spark: The Definitive Guide" 2018, Kleppmann "Designing Data-Intensive Applications" 2017...)

## Phụ lục A: Danh mục tệp nộp kèm {.unnumbered}
Bảng: tên file, nội dung.

## Phụ lục B: Mã nguồn các phần TODO {.unnumbered}
Các đoạn code chính đã hoàn thành (trích, không cần toàn bộ file).
```

Thay `l01` bằng mã lab tương ứng (`l02`…`l06`) trong **mọi** nhãn — nhãn phải duy nhất toàn bộ 6 báo cáo vì sẽ gộp
thành một file.

## Cú pháp pandoc-crossref

- Hình (đoạn riêng, có dòng trống trước/sau):
  `![Chú thích đầy đủ, kết thúc bằng dấu chấm.](img/02_blocks.png){#fig:l01-blocks width=85%}`
  Ảnh terminal cao/hẹp có thể dùng `width=70%`; ảnh UI rộng `width=100%`.
- Bảng: dòng chú thích **ngay sau** bảng, cách một dòng trống:
  ```
  | A | B |
  |---|---|
  | 1 | 2 |

  : Chú thích bảng. {#tbl:l01-blocks}
  ```
- Tham chiếu trong câu: `@fig:l01-blocks`, `@tbl:l01-blocks`, `@sec:l01-results` (sinh ra "Hình 1", "Bảng 2", "Mục 4").
  Viết "như thể hiện ở @fig:l01-blocks" — **không** tự gõ "Hình 3".
- **Mọi** hình và bảng phải được nhắc tới trong văn bản ít nhất một lần.
- Không đặt ảnh bên trong ô bảng. Không dùng HTML.
- Công thức: `$...$` hoặc `$$...$$`, ví dụ $\lceil 300/128 \rceil = 3$.
- Code: fenced block có ngôn ngữ (```bash, ```python, ```sql). Dòng ≤ 100 ký tự nếu có thể.

## Văn phong

- Trang trọng, khách quan, ngôi thứ ba/bị động: "Thực nghiệm cho thấy…", "Kết quả ở @tbl:… chỉ ra…".
  Không dùng "mình", "bạn", "ta", tiếng lóng, emoji, mũi tên "→" trong câu văn (dùng "dẫn đến", "chuyển sang").
- Thuật ngữ tiếng Anh: lần đầu viết *in nghiêng* kèm giải thích tiếng Việt, ví dụ *replication factor* (hệ số nhân bản).
- Viết thành đoạn văn liền mạch; danh sách chỉ dùng khi thực sự liệt kê.
- Số liệu: giữ **chính xác** như trong log/báo cáo cũ (không làm tròn khác đi, không bịa số). Có đơn vị.
  Dùng dấu phẩy ngăn cách hàng nghìn như trong log (2,964,624) và dấu chấm thập phân (4.10 s).
- Mỗi nhận định định lượng phải dựa trên số liệu có trong log (`submission/LXX/*.txt`) hoặc REPORT.md cũ.
- Không xóa thông tin kỹ thuật của báo cáo cũ; được sắp xếp lại, diễn đạt lại, bổ sung giải thích.
- Dùng **tất cả** ảnh trong `submission/LXX/img/` (trừ khi trùng lặp rõ ràng; khi bỏ thì ghi lý do trong tin nhắn trả về).

## Vòng 2: viết như sinh viên thật viết, không như AI (bắt buộc, ưu tiên hơn các mục trên nếu mâu thuẫn)

Người đọc là giảng viên chấm bài. Báo cáo phải đọc như một sinh viên năm cuối tự làm và tự viết.

**Bỏ hẳn**
- Ảnh chụp mã nguồn `img/code_*.png`: không chèn, không nhắc tới.
- Chữ "TODO" trong thân bài. Mô tả việc đã làm bằng lời, ví dụ "hàm reducer cộng dồn số đếm theo khóa", thay vì
  "TODO 2". Chỉ trong Phụ lục B mới được ghi chú tệp và vị trí đoạn mã.
- Các cụm sáo rỗng: "Đáng chú ý", "Cần nhấn mạnh", "Nhìn chung", "Tóm lại", "Có thể thấy rõ", "đóng vai trò quan
  trọng/then chốt", "không chỉ ... mà còn", "chính là", "một cách hiệu quả", "điều này cho thấy rõ ràng",
  "hoàn toàn", "toàn diện". Không mở câu trả lời bằng "**Có.**", "**Không.**".
- Gạch ngang dài "—" trong câu văn. Mũi tên. Emoji.
- Bộ ba liệt kê cho có ("nhanh, gọn và hiệu quả"), câu kết đoạn kiểu tổng kết lại điều vừa nói.

**Tiết chế**
- In đậm: tối đa khoảng 5 chỗ cả bài, chỉ cho con số kết luận quan trọng nhất.
- Danh sách gạch đầu dòng: chỉ khi thật sự là danh sách (các bước quy trình, danh mục). Lập luận viết thành đoạn.
- Chú thích tiếng Việt cho thuật ngữ: chỉ với thuật ngữ ít quen. Các từ dân CNTT vẫn dùng (partition, shuffle, block,
  job, task, stage, cache, join, executor, driver) để nguyên, không in nghiêng, không giải nghĩa.
- Không lặp số liệu: mỗi con số nêu đầy đủ ở một chỗ (bảng hoặc câu), chỗ khác chỉ tham chiếu.
- Độ dài: ngắn hơn bản hiện tại khoảng 20–30 % bằng cách cắt câu lặp ý, câu dẫn dắt thừa ("Như thể hiện ở Hình X,
  kết quả cho thấy..."); viết thẳng "Hình X: ...", hoặc "(@fig:...)" cuối câu.

**Nên**
- Câu dài ngắn xen kẽ, đi thẳng vào ý. Giọng khách quan nhưng tự nhiên; được dùng "trong bài này", "ở lần chạy
  thứ hai", thỉnh thoảng "chúng tôi" khi nói về việc đã làm (ví dụ "chúng tôi chạy lại hai lần để loại ảnh hưởng
  khởi động JVM").
- Nêu cả những chỗ trục trặc thật đã gặp và cách xử lý (lấy từ log/ghi chú cũ, không bịa), đó là dấu hiệu bài tự làm.
- Nhận xét có chính kiến, ngắn: "Kết quả này hơi bất ngờ vì...", "Con số này chưa đủ để kết luận...".
- Cấu trúc mục giữ nguyên như trên (Tóm tắt, Giới thiệu, Cơ sở lý thuyết, Môi trường và phương pháp, Kết quả,
  Thảo luận, Kết luận, Tài liệu tham khảo, Phụ lục). Tiêu đề mục ngắn, không có dấu hai chấm kiểu "X: Y" trừ khi cần.
- Mục Thảo luận vẫn trả lời đủ các câu hỏi checkpoint, giữ nhãn `{#sec:l0X-qN}`, nhưng tiêu đề viết thành một
  câu hỏi tự nhiên ngắn gọn, không cần chữ "Câu hỏi N:".
- Phụ lục B đổi tên thành "Phụ lục B: Trích đoạn mã nguồn {.unnumbered}": chỉ vài đoạn ngắn (mỗi đoạn tối đa khoảng
  15 dòng) thể hiện phần tự viết, kèm một câu giải thích trước mỗi đoạn.

## Vòng 3: báo cáo thực hành gọn, không phải bài nghiên cứu (ưu tiên cao nhất, đè lên các mục trên)

Thứ tự ưu tiên: thực nghiệm cụ thể > kết quả có bằng chứng > nhận xét đúng trọng tâm > lý thuyết bổ trợ.
Sửa trên nội dung hiện có, không viết lại từ đầu khi không cần. Giữ văn phong vòng 2.

**Cấu trúc mới** (bỏ Tóm tắt/Từ khóa, bỏ mục "Cơ sở lý thuyết" riêng):

```markdown
# <Tiêu đề lab> {#sec:l0X}

## Mục tiêu và môi trường {#sec:l0X-intro}
Một đoạn ngắn: lab làm gì, các task chính. Bảng môi trường (giữ bảng cũ nếu có, tinh giản).
Dữ liệu dùng (nguồn, kích thước) gộp vào đây.

## Thực hiện và kết quả {#sec:l0X-results}
### <Task 1 theo đúng thứ tự trong README đề bài> {#sec:l0X-...}
Mỗi task: cần kiểm chứng gì, đã làm gì (lệnh/cấu hình chính), kết quả (bảng/hình/output), nhận xét.
Viết liền thành đoạn, không bắt buộc 4 tiểu mục. Lý thuyết chỉ 1-2 câu, đặt ngay chỗ cần để hiểu kết quả.

## Trả lời câu hỏi checkpoint {#sec:l0X-discussion}
### <câu hỏi ngắn> {#sec:l0X-qN}
Trả lời trực tiếp, viện dẫn kết quả bằng @fig/@tbl/@sec, không chép lại số liệu đã có trong bảng.

## Kết luận {#sec:l0X-conclusion}
Một đoạn: các yêu cầu đã hoàn thành + vài kết quả chính. Không kiến thức mới, không hướng phát triển.
Khuyến nghị production chỉ giữ nếu README/INSTRUCTOR_NOTES của lab yêu cầu (khi đó để ở mục riêng ngắn gọn).

## Tài liệu tham khảo {.unnumbered}
Chỉ các mục thực sự được trích trong bài (thường 2-4). Đánh số lại [1].. và sửa các trích dẫn trong thân bài.

## Phụ lục: Trích đoạn mã nguồn {.unnumbered}
Giữ vài đoạn mã tự viết ngắn (nếu đề yêu cầu hoàn thành TODO). Bỏ bảng danh mục tệp nếu không cần thiết,
hoặc rút còn vài dòng.
```

**Cắt:** định nghĩa/kiến trúc/lịch sử công nghệ dài; công thức giới thiệu không dùng để tính; câu mở đầu và câu
tổng kết lặp ý; mô tả quy trình mà phần kết quả đã mô tả lại; số liệu nhắc lại ở thảo luận/kết luận.

**Giữ nguyên:** mọi lệnh/cấu hình quan trọng, mọi số liệu, bảng, hình (giữ nhãn #fig/#tbl cũ), các kết quả
trước/sau, hiện tượng bất thường và trục trặc thật đã gặp, toàn bộ câu trả lời checkpoint (giữ nhãn qN).
Không bịa kết quả/log/số liệu. Phần stretch/optional chưa làm thì không đưa vào.

**Kỹ thuật:** mọi @fig/@tbl/@sec trong file phải trỏ tới nhãn tồn tại trong cùng file; mọi hình và bảng phải
được nhắc ít nhất một lần. Không chữ TODO trong thân bài, không "—", không mũi tên.
