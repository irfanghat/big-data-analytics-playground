CREATE DATABASE IF NOT EXISTS unsw_db;

USE unsw_db;

CREATE EXTERNAL TABLE IF NOT EXISTS unsw_nb15 (
    srcip STRING,
    sport INT,
    dstip STRING,
    dsport INT,
    proto STRING,
    state STRING,
    dur DOUBLE,
    sbytes BIGINT,
    dbytes BIGINT,
    sttl INT,
    dttl INT,
    sloss INT,
    dloss INT,
    service STRING,
    Sload DOUBLE,
    Dload DOUBLE,
    Spkts INT,
    Dpkts INT,
    swin INT,
    dwin INT,
    stcpb BIGINT,
    dtcpb BIGINT,
    smeansz INT,
    dmeansz INT,
    trans_depth INT,
    res_bdy_len BIGINT,
    Sjit DOUBLE,
    Djit DOUBLE,
    Stime BIGINT,
    Ltime BIGINT,
    Sintpkt DOUBLE,
    Dintpkt DOUBLE,
    tcprtt DOUBLE,
    synack DOUBLE,
    ackdat DOUBLE,
    is_sm_ips_ports INT,
    ct_state_ttl INT,
    ct_flw_http_mthd INT,
    is_ftp_login INT,
    ct_ftp_cmd INT,
    ct_srv_src INT,
    ct_srv_dst INT,
    ct_dst_ltm INT,
    ct_src_ltm INT,
    ct_src_dport_ltm INT,
    ct_dst_sport_ltm INT,
    ct_dst_src_ltm INT,
    attack_cat STRING,
    label INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/unsw/';
