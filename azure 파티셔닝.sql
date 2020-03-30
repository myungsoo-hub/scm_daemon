--azure에서는 파일그룹을 추가할 필요없이 파티셔닝 생성 가능 

IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'MATH')
DROP DATABASE MATH
GO

--데이터베이스 생성
CREATE DATABASE MATH
(
MAXSIZE = 1GB,
EDITION = 'STANDARD',
SERVICE_OBJECTIVE = 'S2'
)
GO   

USE SCMDB
GO

-- 파티션 펑션 만들기(left , RIGHT가 있으며 오름차순으로 정렬할때 기본은 left)
--1. 2019-03-01이전
--2.2019-03-01~2019-03-31까지
--3.2019-04-01~2019-04-31까지
--4.2019-05-01 이후
--CREATE PARTITION FUNCTION PF_HASH_BY_VALUE (varchar(10)) AS RANGE LEFT 


CREATE PARTITION FUNCTION PF_SCM_VALUE (varchar(10)) AS RANGE LEFT 
FOR VALUES ('2018-06-01', '2018-07-01', '2018-08-01', '2018-09-01', '2018-10-01', '2018-11-01', '2018-12-01', '2019-01-01', '2019-02-01', '2019-03-01', '2019-04-01', '2019-05-01', '2019-06-01', '2019-07-01', '2019-08-01', '2019-09-01', '2019-10-01', '2019-11-01', '2019-12-01')
GO

-------------------------
--파티션 수정 시  생성될 값 입력
--ALTER PARTITION FUNCTION PF_SCM_VALUE()  SPLIT  RANGE('2019-08-01')

-- 파티션 그룹 재 생성(수정후 필수 실행)
--ALTER PARTITION SCHEME PS_SCM_VALUE 
--NEXT USED PRIMARY;
-------------------------

--  파티션 펑셩 확인
SELECT * FROM sys.partition_functions
GO

IF EXISTS (SELECT * FROM sys.partition_schemes  
    WHERE name = 'PS_SCM_VALUE')  
DROP PARTITION SCHEME PS_SCM_VALUE;  
GO  

-- primary 파티션 스키마 생성 
CREATE PARTITION SCHEME PS_SCM_VALUE 
AS PARTITION PF_SCM_VALUE
ALL TO (PRIMARY);
GO

-- 스키마 확인
SELECT * FROM sys.partition_schemes
GO

-- 정상적으로 생성하였으며 입력값에 따른 위치 확인
SELECT 
  MY_VALUE,
  $PARTITION.PF_SCM_VALUE(MY_VALUE) AS HASH_IDX
FROM 
(
 VALUES 
   ('2019-03-02'), 
   ('2019-04-03') , 
   ('2019-05-05'),
    ('2019-03-05'), 
    ('2019-02-05'),
	 ('2019-04-30'),
	  ('2019-06-01')
) AS TEST (MY_VALUE);
GO


-- 값이 일력될 테이블 생성
CREATE TABLE dbo.scm_stock_preset 
(
  orderdate varchar(10) NOT NULL,
  storeno char(4) NOT NULL,
  m_code varchar(9) NOT NULL,
ppmidx int NOT NULL,
product_code varchar(10) NOT NULL,
group_susik char(1) NOT NULL CONSTRAINT DF_stock_preset4_group_2018_susik  DEFAULT ('N'),
amt_balju_pre int NULL CONSTRAINT DF_scm_stock_preset_amt_balju_pre  DEFAULT ((0)),
amt_balju int NULL CONSTRAINT DF_scm_stock_preset_amt_result  DEFAULT ((0)),
	amt_sale_d3 int NULL CONSTRAINT DF_scm_stock_preset_amt_sale_d3  DEFAULT ((0)),
	amt_sale_d2 int NULL CONSTRAINT DF_scm_stock_preset_amt_sale_d2  DEFAULT ((0)),
	amt_sale_d1 int NULL CONSTRAINT DF_scm_stock_preset_amt_sale_d1  DEFAULT ((0)),
	amt_sale_d int NULL CONSTRAINT DF_scm_stock_preset_amt_sale_d  DEFAULT ((0)),
	amt_pda_d3 int NULL CONSTRAINT DF_scm_stock_preset_amt_pda_d3  DEFAULT ((0)),
	amt_pda_d2 int NULL CONSTRAINT DF_scm_stock_preset_amt_pda_d2  DEFAULT ((0)),
	amt_pda_d1 int NULL CONSTRAINT DF_scm_stock_preset_amt_pda_d1  DEFAULT ((0)),
	amt_safe_d int NULL CONSTRAINT DF_scm_stock_preset_amt_safe_d  DEFAULT ((0)),
	amt_safe_max int NULL CONSTRAINT DF_scm_stock_preset_amt_safe_max  DEFAULT ((0)),
	amt_magam int NULL CONSTRAINT DF_scm_stock_preset_amt_magam  DEFAULT ((0)),
	amt_input_d3 int NULL CONSTRAINT DF_scm_stock_preset_amt_input_d3  DEFAULT ((0)),
	amt_input_d2 int NULL CONSTRAINT DF_scm_stock_preset_amt_input_d2  DEFAULT ((0)),
	amt_input_d1 int NULL CONSTRAINT DF_scm_stock_preset_amt_input_d1  DEFAULT ((0)),
	amt_adjust1 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust1  DEFAULT ((0)),
	amt_adjust2 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust2  DEFAULT ((0)),
	amt_adjust3 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust3  DEFAULT ((0)),
	amt_adjust4 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust4  DEFAULT ((0)),
	amt_adjust5 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust5  DEFAULT ((0)),
	amt_adjust6 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust6  DEFAULT ((0)),
	amt_adjust7 int NULL CONSTRAINT DF_scm_stock_preset_amt_adjust61  DEFAULT ((0)),
	amt_input_d int NULL CONSTRAINT DF_scm_stock_preset_amt_input_d  DEFAULT ((0)),
	amt_JJ_adjust_d int NULL CONSTRAINT DF_scm_stock_preset_amt_JJ_adjust_d  DEFAULT ((0)),
	amt_office_bnbae int NULL CONSTRAINT DF_scm_stock_preset_amt_office_bnbae  DEFAULT ((0)),
	amt_office_bnbae_ex int NULL CONSTRAINT DF_scm_stock_preset_amt_office_bnbae_ex  DEFAULT ((0)),
	amt_spare_bnbae int NULL CONSTRAINT DF_scm_stock_preset_amt_spare_bnbae  DEFAULT ((0)),
	amt_soldout int NULL CONSTRAINT DF_scm_stock_preset_amt_soldout  DEFAULT ((0)),
	scm_gb char(1) NULL CONSTRAINT DF_scm_stock_preset_scm_gb  DEFAULT ((0)),
	stock_only_ab char(1) NULL,
	scm_susik char(1) NULL,
	amt_bundle int NULL,
	reg_gb char(1) NOT NULL CONSTRAINT DF_scm_stock_preset_reg_gb  DEFAULT ('3'),
  regdate datetime NOT NULL  DEFAULT ('')
) ON PS_SCM_VALUE (orderdate) --파티션  컬럼과 같아야지 됨 



-- 값 입력
insert into dbo.scm_stock_preset values (1, '2019-03-02',getdate())
insert into dbo.scm_stock_preset values (2, '2019-04-03',getdate())
insert into dbo.scm_stock_preset values (3, '2019-05-05',getdate())
insert into dbo.scm_stock_preset values (4, '2019-03-05',getdate())
insert into dbo.scm_stock_preset values (5, '2019-02-05',getdate())
insert into dbo.scm_stock_preset values (6, '2019-04-30',getdate())
insert into dbo.scm_stock_preset values (7, '2019-06-01',getdate())
insert into dbo.scm_stock_preset values (8, '2019-04-07',getdate())
insert into dbo.scm_stock_preset values (9, '2019-01-07',getdate())
insert into dbo.scm_stock_preset values (10, '2019-04-09',getdate())
insert into dbo.scm_stock_preset values (11, '2019-06-07',getdate())
insert into dbo.scm_stock_preset values (12, '2019-02-17',getdate())
insert into dbo.scm_stock_preset values (13, '2019-03-10',getdate())
insert into dbo.scm_stock_preset values (14, '2019-05-10',getdate())
insert into dbo.scm_stock_preset values (15, '2019-09-10',getdate())

--입력된 값 파티션 정보 조회
SELECT 
  Partition_Number, Row_Count 
FROM sys.dm_db_partition_stats
WHERE object_id = object_id('scm_stock_preset');


select * from scm_stock_preset


--삭제 순서
--DROP TABLE scm_stock_preset
--DROP PARTITION SCHEME PS_SCM_VALUE 
--DROP PARTITION FUNCTION PF_SCM_VALUE


