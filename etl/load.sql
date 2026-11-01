CREATE DATABASE IF NOT EXISTS unsw;

CREATE TEMPORARY VIEW raw_unsw
USING csv
OPTIONS (path 'hdfs://namenode:8020/data/unsw/UNSW-NB15.csv', header 'false', inferSchema 'false');

DROP TABLE IF EXISTS unsw.flows;

CREATE TABLE unsw.flows STORED AS PARQUET AS
SELECT
  _c0  AS srcip,
  _c1  AS sport,
  _c2  AS dstip,
  _c3  AS dsport,
  _c4  AS proto,
  _c5  AS state,
  CAST(_c6  AS DOUBLE) AS dur,
  CAST(_c7  AS BIGINT) AS sbytes,
  CAST(_c8  AS BIGINT) AS dbytes,
  CAST(_c9  AS INT)    AS sttl,
  CAST(_c10 AS INT)    AS dttl,
  CAST(_c11 AS INT)    AS sloss,
  CAST(_c12 AS INT)    AS dloss,
  _c13 AS service,
  CAST(_c14 AS DOUBLE) AS sload,
  CAST(_c15 AS DOUBLE) AS dload,
  CAST(_c16 AS INT)    AS spkts,
  CAST(_c17 AS INT)    AS dpkts,
  CAST(_c18 AS INT)    AS swin,
  CAST(_c19 AS INT)    AS dwin,
  CAST(_c20 AS BIGINT) AS stcpb,
  CAST(_c21 AS BIGINT) AS dtcpb,
  CAST(_c22 AS INT)    AS smeansz,
  CAST(_c23 AS INT)    AS dmeansz,
  CAST(_c24 AS INT)    AS trans_depth,
  CAST(_c25 AS INT)    AS res_bdy_len,
  CAST(_c26 AS DOUBLE) AS sjit,
  CAST(_c27 AS DOUBLE) AS djit,
  CAST(_c28 AS BIGINT) AS stime,
  CAST(_c29 AS BIGINT) AS ltime,
  CAST(_c30 AS DOUBLE) AS sintpkt,
  CAST(_c31 AS DOUBLE) AS dintpkt,
  CAST(_c32 AS DOUBLE) AS tcprtt,
  CAST(_c33 AS DOUBLE) AS synack,
  CAST(_c34 AS DOUBLE) AS ackdat,
  CAST(_c35 AS INT)    AS is_sm_ips_ports,
  CAST(_c36 AS INT)    AS ct_state_ttl,
  CAST(_c37 AS INT)    AS ct_flw_http_mthd,
  CAST(_c38 AS INT)    AS is_ftp_login,
  CAST(_c39 AS INT)    AS ct_ftp_cmd,
  CAST(_c40 AS INT)    AS ct_srv_src,
  CAST(_c41 AS INT)    AS ct_srv_dst,
  CAST(_c42 AS INT)    AS ct_dst_ltm,
  CAST(_c43 AS INT)    AS ct_src_ltm,
  CAST(_c44 AS INT)    AS ct_src_dport_ltm,
  CAST(_c45 AS INT)    AS ct_dst_sport_ltm,
  CAST(_c46 AS INT)    AS ct_dst_src_ltm,
  COALESCE(NULLIF(TRIM(_c47), ''), 'Normal') AS attack_cat,
  CAST(_c48 AS INT)    AS label
FROM raw_unsw;

SELECT COUNT(*) AS total_rows FROM unsw.flows;
SELECT attack_cat, COUNT(*) AS n FROM unsw.flows GROUP BY attack_cat ORDER BY n DESC;