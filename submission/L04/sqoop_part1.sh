#!/usr/bin/env bash
set -u
JDBC=jdbc:postgresql://postgres:5432/olist
mkdir -p /lab/work; rm -f /lab/work/.pg_password; echo -n olist > /lab/work/.pg_password; chmod 400 /lab/work/.pg_password
CONN=(--connect "$JDBC" --username olist --password-file file:///lab/work/.pg_password)
NULLS=(--null-string '\\N' --null-non-string '\\N' --input-null-string '\\N' --input-null-non-string '\\N')
hdfs dfs -mkdir -p /data/olist/sqoop 2>/dev/null

echo '### 1.1 sqoop list-tables / eval'
sqoop list-tables "${CONN[@]}" 2>/dev/null
sqoop eval "${CONN[@]}" --query "SELECT order_status, count(*) FROM orders GROUP BY 1 ORDER BY 2 DESC" 2>/dev/null

echo '### 1.2 full import orders: --split-by order_seq -m 4'
T0=$(date +%s.%N)
sqoop import "${CONN[@]}" --table orders --split-by order_seq -m 4 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_full --delete-target-dir > /lab/work/orders_full.log 2>&1
T1=$(date +%s.%N)
grep -E "BoundingValsQuery|number of splits|Running job|Transferred|Retrieved" /lab/work/orders_full.log
python3 -c "print('WALL full orders import (sqoop CLI, incl. JVM+codegen+YARN): %.1f s' % ($T1-$T0))"
echo '$ hdfs dfs -ls /data/olist/sqoop/orders_full'
hdfs dfs -ls /data/olist/sqoop/orders_full 2>/dev/null
echo '$ hdfs dfs -cat part-m-00000 | head -2'
hdfs dfs -cat /data/olist/sqoop/orders_full/part-m-00000 2>/dev/null | head -2
echo '$ rows per file (orders, split on order_seq)'
for f in $(hdfs dfs -ls -C /data/olist/sqoop/orders_full/part-m-* 2>/dev/null); do echo "$f $(hdfs dfs -cat $f 2>/dev/null | wc -l)"; done

echo '### TODO 1.2 customers: --split-by customer_zip_code_prefix -m 4'
sqoop import "${CONN[@]}" --table customers --split-by customer_zip_code_prefix -m 4 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/customers_full --delete-target-dir > /lab/work/customers_full.log 2>&1
grep -E "BoundingValsQuery|number of splits|Retrieved" /lab/work/customers_full.log
echo '$ rows per file (customers, split on customer_zip_code_prefix)'
for f in $(hdfs dfs -ls -C /data/olist/sqoop/customers_full/part-m-* 2>/dev/null); do echo "$f $(hdfs dfs -cat $f 2>/dev/null | wc -l)"; done
sqoop eval "${CONN[@]}" --query "SELECT min(customer_zip_code_prefix), max(customer_zip_code_prefix), width_bucket(customer_zip_code_prefix, 1003, 99990, 4) AS bucket, count(*) FROM customers GROUP BY 3 ORDER BY 3" 2>/dev/null

echo '### 1.3 incremental append (initial load, --last-value 0)'
hdfs dfs -rm -r -f -skipTrash /data/olist/sqoop/orders_append >/dev/null 2>&1
sqoop import "${CONN[@]}" --table orders --split-by order_seq -m 2 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_append \
  --incremental append --check-column order_seq --last-value 0 2>&1 \
  | grep -E "Lower bound|Upper bound|Retrieved|tool.ImportTool:  |--last-value|--check-column|Consider saving" 

echo '### 1.5a incremental lastmodified baseline'
hdfs dfs -rm -r -f -skipTrash /data/olist/sqoop/orders_lastmod >/dev/null 2>&1
sqoop import "${CONN[@]}" --table orders -m 1 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_lastmod \
  --incremental lastmodified --check-column updated_at --last-value '1970-01-01 00:00:00' > /lab/work/lastmod1.log 2>&1
grep -E "Lower bound|Upper bound|Retrieved|--last-value" /lab/work/lastmod1.log
sed -n 's/.*--last-value \(.*\)$/\1/p' /lab/work/lastmod1.log | tail -1 > /lab/work/lastmod_value.txt
echo "next --last-value: $(cat /lab/work/lastmod_value.txt)"

echo '### 1.4 saved job orders_incr'
sqoop job --delete orders_incr >/dev/null 2>&1
sqoop job --create orders_incr -- import "${CONN[@]}" --table orders --split-by order_seq -m 1 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_append \
  --incremental append --check-column order_seq --last-value 5000 2>/dev/null
echo '$ sqoop job --list'; sqoop job --list 2>/dev/null
echo '$ sqoop job --show orders_incr | grep incremental'; sqoop job --show orders_incr 2>/dev/null | grep incremental
