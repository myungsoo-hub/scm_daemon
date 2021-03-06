USE [db003001]
GO
/****** Object:  StoredProcedure [dbo].[usp_mileage_resave_search]    Script Date: 2019/02/27 16:16:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/************************************************************************
SP이름 : usp_mileage_resave
소속DB : db003001
설      명 : 마일리지 취소 소멸 적립금액 조회
수정기록
2019.02.28. 초기생성/김명수
************************************************************************/
ALTER PROCEDURE [dbo].[usp_mileage_resave_search]
	@parm_mcode varchar(10),
	@parm_orderdate varchar(10),
	@parm_orderno varchar(20)
AS
BEGIN
	Declare @point_use  decimal(10,0)
	Declare @final_point  decimal(10,0)
	Declare @mstCidx int
	Declare @final_mileage_use decimal(10,0)
	Declare @mileage_final decimal(10,0)
	Declare @mileage decimal(10,0)

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	set @point_use = 0
	set @final_point = 0
	set @mstCidx = ''
	set @final_mileage_use = 0
	set @mileage_final  = 0
	set @mileage = 0

	declare @total_maileage_search table
	(
		idx int IDENTITY(1,1) NOT NULL,
		odate   varchar(10) Default 0 , --적립일
		m_code   varchar(15) Default 0 , --적립일
		save_point   decimal(9,0) Default 0 ,	--적립금액
		final_point  decimal(9,0) Default 0,		--현재잔액
		exp_date2  varchar(10) Default 0			--소멸날짜
	)

	Insert Into @total_maileage_search (odate,m_code,save_point, final_point, exp_date2)
	Select odate,m_code,save_point,final_point,exp_date2
	from mstCustMileage 
	where m_code = @parm_mcode  and save_point <> final_point order by idx asc

	select @point_use = ABS(sum(point_use)) from icoop_Emoney_mileage where  m_code = @parm_mcode and orderdate = @parm_orderdate  and SUBSTRING(etc_erp_field1,0,15) = @parm_orderno 

	if @point_use > 0 
	BEGIN

		--SELECT top 1 @reserve_ch = reserve_point from @total_maileage_search where m_code = @parm_mcode and reserve_point <> 0 order by idx asc , exp_date2 asc

			while (	SELECT count(idx) from @total_maileage_search where m_code = @parm_mcode ) > 0
			BEGIN
				
				select  top 1 @mstCidx = idx, @final_point = save_point from @total_maileage_search where m_code = @parm_mcode and save_point <> final_point order by idx desc , exp_date2 desc

				if @point_use > @final_point --물품 금액보다 보상비 현재금액의 포인트가 많으면  5000 > 0
				BEGIN
					SET @final_mileage_use =  @final_point  -- 10000 = 0
					SET @point_use = @point_use - @final_mileage_use   --  20000 - 4000-0
				END
				ELSE
				BEGIN --5000 >6000
					SET @final_mileage_use  =  @point_use  
					SET @point_use = @point_use - @final_mileage_use       
				END
				
				SET @mileage_final = @final_point - @final_mileage_use  
				SET @mileage = @final_mileage_use 	            

				--update mstCustmileage set final_point = @mileage_final, reserve_point = @mileage,finalUpdate = getdate() where idx = @mstCidx
				update @total_maileage_search set  final_point = @mileage  where idx = @mstCidx
		
				if @point_use = 0  and @mileage_final >= 0
				BEGIN			
					break
				END
		
			END

			--select * from @total_maileage_search
			select  sum(final_point)  from @total_maileage_search where exp_date2 <=  convert(varchar(10),getdate(),23)
			--select sum(final_point), min(exp_date2) from @total_maileage_search where exp_date2 <= '2019-03-07'
	END

END
