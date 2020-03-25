using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;


/*
 * 2019-07-10
 * 반올림 수식 변경
 * preset_sale_ppm_daily 값이 있을경우 없으면 pos_sale_A01_ppm_daily 정가판매량으로 생성
 * pos_sale_A01_ppm_daily에서는 낱개박스로 연결된 정가판매량이 없으므로 낱개박스물품일경우 오차 발생
 *
 */

namespace SCM_Auto_baju
{
    public class Sales_forecast
    {
        SqlConnection myConnection = new SqlConnection();
        MySqlConnection MYconn = new MySqlConnection();

        public void fn_forecast_V3_D3(string orderdate,string day_orderdate, Form1 frm1)
        {
            //대상물품 추출(1팀 프로시저)
          fn_get_preset_data(orderdate, frm1);

            //[판매예측값] 생성
            fn_set_sale_forecast_data(orderdate, day_orderdate, "N", frm1);
        }


        public void fn_preset_V3_D3_group(string orderdate ,string day_orderdate, Form1 frm1)
        {
            //그룹물품 추출(1팀 프로시저) => 테스트 데이터 8/29 부터 있음
            fn_get_preset_group_data(orderdate, frm1);

            //[그룹단위판매예측값] 생성
            fn_set_sale_forecast_data(orderdate, day_orderdate,"G", frm1);
        }


        #region 판매예측 1팀 추출
        private void fn_get_preset_data(string orderdate, Form1 frm1)
        {
            // ★[대상물품] 데이터 추출 (1팀 프로시저 호출)★
            // => 대상물품(공급가능,보임인 물품)의 [분배구분/마감여부/취급권역/일자별단가/가용량/주문량/점간이동여부/장보기수량제한여부/묶음단위수량] 데이터 추출
            // => D-6 처리: 최초 대상물품 추출. 
            // => D-3 처리: D-6 이후 필드값이 변경되거나 추가된 물품이 존재할 수도 있으므로 재실행
            // => 테이블명: preset_base_product

            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("164");
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            if (!orderdate.Equals(""))
            {
                String sql = null;
                String g_code = null;
                string prebalju_ab = null;
                String divide_gb = null;
                String magam_gb = null;
                String not_area = null;
                String price = null;
                String p_ab = null;
                String amt_cannum = null;
                String amt_order = null;
                String amt_limit = null;
                String bundle_gb = null;
                String amt_bundle = null;

                MySqlDataAdapter adapter_my = new MySqlDataAdapter();
                SqlDataAdapter adapter = new SqlDataAdapter();
                SqlCommand command = new SqlCommand();
                DataSet ds = new DataSet();

                try
                {
                    //1. vc = '0' 먼저 초기화
                    sql = "UPDATE preset_base_product SET vc = '0' WHERE sdate = '" + orderdate + "'";
                    command.Connection = myConnection;
                    command.CommandText = sql;
                    command.CommandTimeout = 120;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    frm1.sendSms("[대상물품추출1_v3(fn_get_preset_data)]오류:" + ex.Message);
                    frm1.Log("[대상물품추출1_v3(fn_get_preset_data)]오류:" + ex.Message);
                }

                try
                {
                    //2. 프로시저 호출
                    string queryString = "CALL usp_scm001_20180427 ('" + orderdate.Replace("-", "") + "',UNIX_TIMESTAMP(NOW()))";
                    adapter_my.SelectCommand = new MySqlCommand(queryString, MYconn);
                    adapter_my.SelectCommand.CommandTimeout = 120;
                    adapter_my.Fill(ds);
                    DataTable table = ds.Tables[0];
                    DataRowCollection rows = table.Rows;

                    /*
                    [prebalju_ab]
                    A:확정발주1
                    B:확정발주2
                    C:예측발주
                    D:확정발주3
                    I:확정발주4
                    Z:미지정 
                    */

                    foreach (DataRow dr in rows)
                    {
                        g_code = dr["g_code"].ToString();
                        prebalju_ab = dr["prebalju_ab"].ToString();
                        divide_gb = dr["divide_yn"].ToString();
                        magam_gb = dr["magam"].ToString();
                        not_area = dr["noa_code"].ToString();
                        price = dr["price"].ToString();
                        p_ab = dr["p_ab"].ToString();
                        amt_cannum = dr["amount1"].ToString();
                        amt_order = dr["amount2"].ToString();
                        amt_limit = dr["ordernum"].ToString();
                        bundle_gb = dr["moq_on"].ToString();
                        amt_bundle = dr["moq"].ToString();


                        //D-6, D-3 실행하므로 기존에 있는 물품인 경우, 설정값 Update 없는 물품인 경우 Insert
                        sql = "IF EXISTS (SELECT * FROM preset_base_product WHERE sdate = '" + orderdate + "' AND product_code='" + g_code + "')"
                                + " BEGIN "
                                + "     UPDATE preset_base_product"
                                + "     SET prebalju_ab = '" + prebalju_ab + "', divide_gb = '" + divide_gb + "', magam_gb = '" + magam_gb + "', not_area = '" + not_area + "',"
                                + "     price = '" + price + "', p_ab = '" + p_ab + "', amt_cannum = '" + amt_cannum + "',"
                                + "     amt_order = '" + amt_order + "', amt_limit = '" + amt_limit + "', bundle_gb = '" + bundle_gb + "', amt_bundle = '" + amt_bundle + "', vc = '1',  finalUpdate = getdate()"
                                + "     WHERE sdate = '" + orderdate + "' AND product_code = '" + g_code + "'"
                                + " END"
                                + " ELSE"
                                + " BEGIN"
                                + "     INSERT INTO preset_base_product "
                                + "     (sdate, product_code, prebalju_ab, divide_gb, magam_gb, not_area, price, p_ab, amt_cannum, amt_order, amt_limit, bundle_gb, amt_bundle, vc, regdate)"
                                + "     VALUES ('" + orderdate + "', '" + g_code + "', '" + prebalju_ab + "', '" + divide_gb + "', '" + magam_gb + "', '" + not_area + "', '" + price + "', '" + p_ab + "', "
                                + "     '" + amt_cannum + "', '" + amt_order + "', '" + amt_limit + "', '" + bundle_gb + " ', '" + amt_bundle + "', '1',  getdate())"
                                + " END";

                        command.Connection = myConnection;
                        command.CommandText = sql;
                        command.CommandTimeout = 120;
                        command.ExecuteNonQuery();

                    }
                    frm1.Log("[대상물품추출_v3(fn_get_preset_data)]완료");
                }
                catch (Exception ex)
                {
                    frm1.sendSms("[대상물품추출2_v3(fn_get_preset_data)]오류:" + ex.Message);
                    frm1.Log("[대상물품추출2_v3(fn_get_preset_data)]오류:" + ex.Message);
                }
            }
            else
            {
                frm1.sendSms("[대상물품추출3_v3(fn_get_preset_data)]오류:orderdate 없음");
                frm1.Log("[대상물품추출3_v3(fn_get_preset_data)]오류:orderdate 없음");
            }

            MYconn.Dispose();
            myConnection.Dispose();

            MYconn.Close();
            myConnection.Close();

            frm1.Log((String.Format("[MySql [대상물품] 데이터 추출 ]:{0}", MYconn.State.ToString())));
            frm1.Log((String.Format("[MsSql [대상물품] 데이터 추출 ]:{0}", myConnection.State.ToString())));

        }
        #endregion 

        #region 판매예측 생성
        private void fn_set_sale_forecast_data(string orderdate, string day_orderdate, string gubun, Form1 frm1)
        {

            // ★[판매예측값]/[그룹단위판매예측값] 생성★
            // => 생성일자의 4주간+7일간 정가판매량의 평균값 생성
            // => 수식: (4주간 동일요일 정가판매량 합계)/(판매발생일) * 0.4 + (7일간 정가판매량 합계)/(판매발생일) * 0.6
            // => 일반물품/통합물품 분리하여 처리함. (통합물품의 경우 서브코드별 판매량을 모두 합산해야함)
            // => 테이블명: preset_sale_data, preset_sale_ppm_daily (pos_sale_A01_ppm_daily)
            string sql = null;
            string tmp = null;
            string tmp2 = null;//데이터 확인 쿼리
            string pspd_orderdate = "";
            string teble_sale_data = null;

            
           teble_sale_data = "preset_sale_data"; //판매예측값이 등록되고 있는 테이블
           

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlCommand command = new SqlCommand(sql, myConnection);
            SqlDataAdapter adapter = new SqlDataAdapter();
            SqlDataAdapter adapter2 = new SqlDataAdapter();

            

            if (!orderdate.Equals(""))
            {

                //SqlCommand command = new SqlCommand();
                int cnt = 0;

                tmp = "IF OBJECT_ID('tempdb.dbo.#preset_data_7day') IS NOT NULL"
                 + " DROP TABLE #preset_data_7day";
                command.CommandText = tmp;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();

                tmp = "CREATE TABLE  #preset_data_7day (sdate date, storeno char(4), ppmidx varchar(10), amt_sale_first float, amt_sale float, regdate datetime,finalAdmin char(1))";
                command.CommandText = tmp;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();


                tmp = "IF OBJECT_ID('tempdb.dbo.#preset_data_4week') IS NOT NULL" //4주간데이터
               + " DROP TABLE #preset_data_4week";
                command.CommandText = tmp;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();

                tmp = "CREATE TABLE  #preset_data_4week (sdate date, storeno char(4), ppmidx varchar(10), amt_sale_first float, amt_sale float, regdate datetime,finalAdmin char(1))";
                command.CommandText = tmp;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();


                //1. [date_d_w]의 4주간 동일요일의 일자 구하기
                string date_d_w4 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((orderdate)).AddDays(-28));
                string date_d_w3 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((orderdate)).AddDays(-21));
                string date_d_w2 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((orderdate)).AddDays(-14));
                string date_d_w1 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((orderdate)).AddDays(-7));

                //2. [date_d_d]의 7일간의  일자 구하기
                string date_d_d7 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-7));
                string date_d_d6 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-6));
                string date_d_d5 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-5));
                string date_d_d4 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-4));
                string date_d_d3 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-3));
                string date_d_d2 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-2));
                string date_d_d1 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-1));  //값 확인

                //3. preset_sale_ppm_daily.[date_d_del] 삭제할 일자 구하기
                string date_d_del = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((day_orderdate)).AddDays(-30));
                //  string date_d_d1 = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((orderdate)).AddDays(-1));

                string holiday = String.Format("{0:yyyyMMdd}", Convert.ToDateTime(orderdate));
                // string orderdate = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((orderdate)).AddDays(5));

                string scm_table_w4 = "";
                string scm_table_d7 = "";
                string scm_table = "";

                tmp2 = "SELECT  top 1 orderdate from preset_sale_ppm_daily where orderdate =  '" + date_d_d1 + "' ";  //생성일로부터 -1일 날짜가 있는지 확인 쿼리
                command.CommandText = tmp2;
                SqlDataReader rd3 = command.ExecuteReader();
                while (rd3.Read())
                {
                    pspd_orderdate =  rd3["orderdate"].ToString(); //저장된 정보가 있는지 확인
                }
                rd3.Close();

                if (pspd_orderdate != "")
                {
                    scm_table_w4 = " ( SUM(ISNULL(qty_jungga,0)) ) * 0.4 as qty_total "; //있으면 생성된 테이블로 조회
                    scm_table_d7 = " ( SUM(ISNULL(qty_jungga,0)) ) * 0.6 as qty_total "; //있으면 생성된 테이블로 조회
                    scm_table = "preset_sale_ppm_daily  ";
                }
                else
                {
                    scm_table_w4 = " ( SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.4 as qty_total "; //없으면 기존 테이블로 조회
                    scm_table_d7 = " ( SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.6 as qty_total "; //없으면 기존 테이블로 조회
                    scm_table = "pos_sale_A01_ppm_daily   ";
                }
                   // string date_d2 = String.Format("{0:yyyyMMdd}", Convert.ToDateTime(orderdate));

                try
                {

                    try
                    {
                        //  string queryString = " SELECT storeno, ppmidx from preset_product_store where regdate >= '" + orderdate + "' ";
                        string queryString = " SELECT storeno, ppmidx, pcmidx from preset_product_store order by storeno desc";
                        adapter.SelectCommand = new SqlCommand(queryString, myConnection);


                        string storeno = null;
                        string ppmidx = null;
                        string pcmidx = null;

                        DataSet ds2 = new DataSet();
                        adapter.Fill(ds2);
                        DataTable table2 = ds2.Tables[0];
                        DataRowCollection rows2 = table2.Rows;

                        foreach (DataRow dr in rows2)
                        {
                            storeno = dr["storeno"].ToString();
                            ppmidx = dr["ppmidx"].ToString();
                            pcmidx = dr["pcmidx"].ToString();

                            //string pps_sql = " and storeno = '" + storeno + "' and ppmidx  in (" + ppmidx + ") ";
                            //string pps_sql2 = " and  right(storeno,4) = '" + storeno + "' and ppmidx  in (" + ppmidx + ") ";
                            string pps_sql = " and storeno = '" + storeno + "' ";
                            string ppm_sql = "  pcmidx  in (" + pcmidx + ") ";

                            string pps_sql2 = " and  storeno = '" + storeno + "'  ";
                            string G_pps_sql3 = "and a.ppmidx_fg  in (select idx from pos_product_master where " + ppm_sql+")";
                            string ppm_sql3 = "and ppmidx  in (select convert(varchar,idx) from pos_product_master where " + ppm_sql + ")"; //기존에 등록된 물품 삭제를 위한 쿼리

                            //string N_sql = "";
                            //string G_sql = "";

                            if (gubun.Equals("N"))
                            {

                                //preset_base_product 에 있는 물품들의 판매예측값 만들기. 
                                //3.최초 생성 INSERT. 존재하면 
                                //

                                //일반 물품 중 등록되어 있으면 삭제
                         
                                //if (int.Parse(holiday) >= 20190915 && int.Parse(holiday) <= 20190928)  //명절에만 제외
                                //{
                                //    N_sql  = " WHERE t1.storeno NOT IN ('6135','6246','6122','6247','6245') ";
                                //    G_sql = " AND t1.storeno NOT IN  ('6135','6246','6122','6247','6245') ";

                                //}
                                //else
                                //{
                                 
                                    sql = "IF EXISTS (SELECT * FROM " + teble_sale_data + " WHERE sdate= '" + orderdate + "' " + pps_sql + " " + ppm_sql3 + ") "
                                  + " BEGIN"
                                  + " INSERT INTO preset_sale_data_back (sdate, storeno, ppmidx, amt_sale_first, amt_sale,finalUpdate,finalAdmin)  "
                                  + "  SELECT sdate, storeno, ppmidx, amt_sale_first, amt_sale,getdate(),'DEMON_2'  "
                                  + " FROM " + teble_sale_data + "  WHERE sdate = '" + orderdate + "' " + pps_sql + "  " + ppm_sql3 + " "
                                   + " DELETE FROM " + teble_sale_data + " WHERE sdate = '" + orderdate + "' " + pps_sql + " " + ppm_sql3 + "   "
                                  + " END";

                                    command.Connection = myConnection;
                                    command.CommandText = sql;
                                    command.CommandTimeout = 120;
                                    command.ExecuteNonQuery();

                                    //테스트 그룹물품 중 등록되어 있으면 삭제 
                                    sql = "IF EXISTS (SELECT * FROM " + teble_sale_data + " WHERE sdate= '" + orderdate + "' " + pps_sql + "	AND ppmidx IN (SELECT 	 CONVERT(varchar, a.group_cd)   FROM pos_erp_goods_group as a INNER JOIN pos_product_master as b ON a.product_cd = b.icoop_code WHERE ISNULL(a.sdate, '') <=   '" + orderdate + "'  AND (ISNULL(a.edate, '') = '' OR ISNULL(a.edate, '') >= '" + orderdate + "') AND  " + ppm_sql + ")) "
                                          + " BEGIN"
                                          + " INSERT INTO preset_sale_data_back (sdate, storeno, ppmidx, amt_sale_first, amt_sale,finalUpdate,finalAdmin)  "
                                          + "  SELECT sdate, storeno, ppmidx, amt_sale_first, amt_sale,getdate(),'DEMON_2'  "
                                          + " FROM " + teble_sale_data + "  WHERE sdate = '" + orderdate + "' " + pps_sql + " 	AND ppmidx IN (SELECT 	 CONVERT(varchar, a.group_cd)   FROM pos_erp_goods_group as a INNER JOIN pos_product_master as b ON a.product_cd = b.icoop_code WHERE ISNULL(a.sdate, '') <=   '" + orderdate + "'  AND (ISNULL(a.edate, '') = '' OR ISNULL(a.edate, '') >= '" + orderdate + "') AND  " + ppm_sql + ") "
                                           + " DELETE FROM " + teble_sale_data + " WHERE sdate = '" + orderdate + "' " + pps_sql + " 	AND ppmidx IN (SELECT 	 CONVERT(varchar, a.group_cd)   FROM pos_erp_goods_group as a INNER JOIN pos_product_master as b ON a.product_cd = b.icoop_code WHERE ISNULL(a.sdate, '') <=   '" + orderdate + "'  AND (ISNULL(a.edate, '') = '' OR ISNULL(a.edate, '') >= '" + orderdate + "') AND  " + ppm_sql + ")  "
                                          + " END";

                                    command.Connection = myConnection;
                                    command.CommandText = sql;
                                    command.CommandTimeout = 120;
                                    command.ExecuteNonQuery();
                               // }

                      



                                //최근 일주일 판매량 임시테이블 생성 후 입력
                                tmp = "   INSERT INTO #preset_data_7day (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                               + " SELECT '" + orderdate + "', t1.storeno, t1.ppmidx,"
                                               + "     ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0)," 
                                               + "     ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0), getdate()"
                                               + "     FROM ("
                                               + "             SELECT storeno, ppmidx, orderdate, "
                                                //  +"               ( SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.6 as qty_total     " //7일 평균  *60%
                                               // + "             FROM pos_sale_A01_ppm_daily as a with (nolock)"
                                               + "              "+scm_table_d7+" "
                                               + "              FROM " + scm_table + " as a with (nolock) "
                                               + "             WHERE orderdate in ('" + date_d_d7 + "','" + date_d_d6 + "','" + date_d_d5 + "','" + date_d_d4 + "','" + date_d_d3 + "','" + date_d_d2 + "','" + date_d_d1 + "')      "
                                               + "             " + pps_sql2 + "  " // 테스트 매장
                                               + "             AND NOT EXISTS (SELECT * FROM pos_erp_goods_rep WHERE (ppmidx_rep = a.ppmidx OR ppmidx_fg = a.ppmidx) AND del_yn = 'N' AND p_ab = 'A')"
                                               //미리생성된 경우도 제외
                                               //+ "             AND NOT EXISTS (SELECT * FROM #preset_data_7day WHERE sdate = '" + orderdate + "' AND a.storeno = storeno AND a.ppmidx = ppmidx)"
                                               //+ "             AND NOT EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND right(a.storeno,4) = storeno AND a.ppmidx = ppmidx)"
                                               + "             AND ppmidx IN (SELECT idx FROM pos_product_master as a INNER JOIN preset_base_product as b ON a.icoop_code = b.product_code WHERE b.vc = '1' AND b.sdate = '" + orderdate + "' AND "+ppm_sql+")"
                                               + "             AND ppmidx not IN ( select ppmidx from preset_scm_safe_sale_date where  storeno = '" + storeno + "'   and (qty_jungga <= 7 and  qty_safe <=3)  )    " // 기준 안전재고, 정가판매 수량에 포함된 물품 제외
                                                + "             GROUP BY storeno, ppmidx, orderdate"
                                               + "     ) as t1"
                                            //    + " "+N_sql+" "  //명절 제외 요청
                                               + "     GROUP BY t1.storeno, t1.ppmidx"
                                               + "     HAVING ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0) <> 0"
                                               + "     ORDER BY t1.storeno, t1.ppmidx"

                                                + "     INSERT INTO #preset_data_7day (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                                + "     SELECT '" + orderdate + "', t1.storeno, t1.ppmidx_rep,"
                                                + "    ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0)," //최대값 재외
                                                + "    ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0), getdate()"
                                                + "     FROM ("     //테이블 b에 있는 물품만 생성
                                                + "               SELECT storeno, ppmidx_rep, orderdate, "
                                               // +"              (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.6 as qty_total   " //7일 평균  *60%
                                                 + "              " + scm_table_d7 + " "
                                                + "             FROM pos_erp_goods_rep as a"
                                              //  + "             INNER JOIN pos_sale_A01_ppm_daily as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                                + "             INNER JOIN "+ scm_table + "  as b with(nolock) ON a.ppmidx_fg = b.ppmidx "
                                                + "             WHERE b.orderdate in ('" + date_d_d7 + "','" + date_d_d6 + "','" + date_d_d5 + "','" + date_d_d4 + "','" + date_d_d3 + "','" + date_d_d2 + "','" + date_d_d1 + "')  "
                                                 + "             " + pps_sql2 + "  "// 테스트 매장과 물품들 쿼리
                                                + "             AND ppmidx IN (SELECT idx FROM pos_product_master as a INNER JOIN preset_base_product as b ON a.icoop_code = b.product_code WHERE b.vc = '1' AND b.sdate = '" + orderdate + "' AND " + ppm_sql + ")"
                                                + "             AND ppmidx not IN ( select ppmidx from preset_scm_safe_sale_date where  storeno = '" + storeno + "'  and (qty_jungga <= 7 and  qty_safe <=3)   )    "     //기준 안전재고, 정가판매 수량에 포함된 물품 제외
                                                //미리생성된 경우도  제외
                                                //+ "             AND  NOT EXISTS (SELECT * FROM #preset_data_7day WHERE sdate = '" + orderdate + "' AND right(b.storeno,4) = storeno AND CONVERT(varchar, a.ppmidx_rep) = ppmidx)"
                                                // + "             AND  NOT EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND right(b.storeno,4) = storeno AND CONVERT(varchar, a.ppmidx_rep) = ppmidx)"
                                                + "             AND a.del_yn = 'N' AND a.p_ab = 'A'"
                                                + "             GROUP BY b.orderdate,  a.ppmidx_rep, b.storeno"
                                                + "     ) as t1"
                                                + "     GROUP BY t1.storeno, t1.ppmidx_rep"
                                                + "     HAVING ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END,0) <> 0"
                                                + "     ORDER BY t1.storeno, t1.ppmidx_rep";
                                command.Connection = myConnection;
                                command.CommandText = tmp;
                                command.CommandTimeout = 120;
                                command.ExecuteNonQuery();


                                //전주 4주 판매량 평균
                                //  sql = "INSERT INTO preset_sale_data (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate)"
                                // sql = "INSERT INTO "+ teble_sale_data + " (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate)"
                                sql = "INSERT INTO #preset_data_4week (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate)"
                                        + "     SELECT '" + orderdate + "', t1.storeno, t1.ppmidx, "
                                        + "   ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0)," //최대값 재외
                                        + "   ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0), getdate()"
                                        + "     FROM ("
                                        + "              SELECT storeno, ppmidx, orderdate, "
                                          //+"              (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.4 as qty_total    " //4주 평균  *40%
                                        // + "             FROM pos_sale_A01_ppm_daily as a with (nolock)"
                                        + "            "+ scm_table_w4 + "    " //4주 평균  *40%
                                         + "           FROM  " + scm_table + "   as a with (nolock)"  
                                        + "             WHERE orderdate in ('" + date_d_w4 + "','" + date_d_w3 + "','" + date_d_w2 + "','" + date_d_w1 + "') "
                                        + "             " + pps_sql2 + "  "// 테스트 매장과 물품들 쿼리
                                        + "             AND NOT EXISTS (SELECT * FROM pos_erp_goods_rep WHERE (ppmidx_rep = a.ppmidx OR ppmidx_fg = a.ppmidx) AND del_yn = 'N' AND p_ab = 'A')"
                                       //   + "             AND  NOT EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND right(a.storeno,4) = storeno AND a.ppmidx = ppmidx)"
                                        //+ "             AND  NOT EXISTS (SELECT * FROM #preset_data_4week  WHERE sdate = '" + orderdate + "' AND right(a.storeno,4) = storeno AND a.ppmidx = ppmidx)"
                                        + "             AND ppmidx IN (SELECT idx FROM pos_product_master as a INNER JOIN preset_base_product as b ON a.icoop_code = b.product_code WHERE b.vc = '1' AND b.sdate = '" + orderdate + "' AND " + ppm_sql + ")"
                                        + "             AND ppmidx not IN ( select ppmidx from preset_scm_safe_sale_date where storeno = '" + storeno + "'   and (qty_jungga <= 7 and  qty_safe <=3)  )    "    //기준 안전재고, 정가판매 수량에 포함된 물품 제외
                                        + "             GROUP BY storeno, ppmidx, orderdate"
                                        + "     ) as t1"
                                    //    + " " + N_sql + " "  //명절 제외 요청
                                        + "     GROUP BY t1.storeno, t1.ppmidx"
                                        + "     HAVING ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0) <> 0"
                                        + "     ORDER BY t1.storeno, t1.ppmidx"

                                       // + "     INSERT INTO preset_sale_data (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                       //+ "     INSERT INTO "+ teble_sale_data+" (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                       + "     INSERT INTO #preset_data_4week (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                        + "     SELECT '" + orderdate + "', t1.storeno, t1.ppmidx_rep,"
                                        + "    ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0),"   //최대값 재외
                                        + "    ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0), getdate()"
                                        + "     FROM ("     //테이블 b에 있는 물품만 생성
                                        + "              SELECT b.storeno, a.ppmidx_rep, b.orderdate, "
                                        //+ "             (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.4 as qty_total"  //4주 평균  *40%
                                        + "               " + scm_table_w4 + "  "  //4주 평균  *40%
                                        + "             FROM pos_erp_goods_rep as a"
                                        //+ "             INNER JOIN pos_sale_A01_ppm_daily as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                        + "             INNER JOIN "+ scm_table +" as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                        + "             WHERE b.orderdate in ('" + date_d_w4 + "','" + date_d_w3 + "','" + date_d_w2 + "','" + date_d_w1 + "')  "
                                        + "             " + pps_sql2 + "  "// 테스트 매장과 물품들 쿼리
                                        + "             AND ppmidx IN (SELECT idx FROM pos_product_master as a INNER JOIN preset_base_product as b ON a.icoop_code = b.product_code WHERE b.vc = '1' AND b.sdate = '" + orderdate + "' AND " + ppm_sql + ")"
                                        + "             AND ppmidx not IN ( select ppmidx from preset_scm_safe_sale_date where storeno = '" + storeno + "'   and (qty_jungga <= 7 and  qty_safe <=3)  )    "  //기준 안전재고, 정가판매 수량에 포함된 물품 제외
                                       // + "             AND  NOT EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND right(b.storeno,4) = storeno AND CONVERT(varchar, a.ppmidx_rep) = ppmidx)"
                                       //  + "             AND  NOT EXISTS (SELECT * FROM #preset_data_4week  WHERE sdate = '" + orderdate + "' AND right(b.storeno,4) = storeno AND CONVERT(varchar, a.ppmidx_rep) = ppmidx)"
                                        + "             AND a.del_yn = 'N' AND a.p_ab = 'A'"
                                        + "             GROUP BY b.orderdate,  a.ppmidx_rep, b.storeno"
                                        + "     ) as t1"
                                        + "     GROUP BY t1.storeno, t1.ppmidx_rep"
                                        + "     HAVING ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END,0) <> 0"
                                        + "     ORDER BY t1.storeno, t1.ppmidx_rep";
                            }
                            else if (gubun.Equals("G")) //그룹테스트 해야됨!!!!!!!!!!!
                            {

                                //일주일간 판매량 계산 임시테이블
                                tmp = "   INSERT INTO #preset_data_7day (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                       + "     SELECT '" + orderdate + "', t1.storeno, t1.group_cd, "
                                       + "   ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0),"   //최대값 재외
                                       + "   ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0), getdate()"
                                       + "     FROM ("
                                       + "             SELECT tbl.storeno, tbl2.group_cd, tbl.orderdate, SUM(tbl.qty_total) as qty_total FROM ("
                                       //그룹내 통합코드 물품들의  7일간 정가판매량
                                       + "                 SELECT b.storeno, a.ppmidx_rep as product_ppmidx, b.orderdate, "
                                         //+"                   (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.6 as qty_total"
                                       + "              " + scm_table_d7 + " "
                                       + "                 FROM pos_erp_goods_rep as a"
                                        //+ "                 INNER JOIN pos_sale_A01_ppm_daily as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                       + "                 INNER JOIN "+ scm_table + " as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                       + "                 WHERE orderdate in  ('" + date_d_d7 + "','" + date_d_d6 + "','" + date_d_d5 + "','" + date_d_d4 + "','" + date_d_d3 + "','" + date_d_d2 + "','" + date_d_d1 + "')"
                                       + "             " + pps_sql2 + "  "// 테스트 매장
                                       + "              " + G_pps_sql3 + "" //그룹메장은 카테고리 조건이 없어서 추가
                                       + "                 AND a.rep_g_code IN (SELECT product_cd FROM pos_erp_goods_group WHERE ISNULL(sdate, '') <= '" + orderdate + "' AND (ISNULL(edate, '') = '' OR ISNULL(edate, '') >= '" + orderdate + "'))"
                                       + "                 AND a.del_yn = 'N' AND a.p_ab = 'A'"
                                       + "                 GROUP BY b.orderdate,  a.ppmidx_rep, b.storeno"
                                       + "                 UNION ALL"
                                       //그룹내 일반물품들의 7일간 정가판매량
                                       + "                 SELECT storeno, ppmidx as product_ppmidx, orderdate,"
                                        //+"                   (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.6 as qty_total"
                                       + "              " + scm_table_d7 + " "
                                        //+ "                 FROM pos_sale_A01_ppm_daily as a with (nolock)"
                                       + "           FROM  " + scm_table + "   as a with (nolock)"
                                       + "                 WHERE orderdate in  ('" + date_d_d7 + "','" + date_d_d6 + "','" + date_d_d5 + "','" + date_d_d4 + "','" + date_d_d3 + "','" + date_d_d2 + "','" + date_d_d1 + "')"
                                       + "             " + pps_sql2 + "  "// 테스트 매장과 물품들 쿼리
                                       + "                 AND NOT EXISTS (SELECT * FROM pos_erp_goods_rep WHERE (ppmidx_rep = a.ppmidx OR ppmidx_fg = a.ppmidx) AND del_yn = 'N' AND p_ab = 'A')"
                                       + "                 AND ppmidx IN (SELECT b.idx FROM pos_erp_goods_group as a INNER JOIN pos_product_master as b ON a.product_cd = b.icoop_code WHERE ISNULL(a.sdate, '') <= '" + orderdate + "' AND (ISNULL(a.edate, '') = '' OR ISNULL(a.edate, '') >= '" + orderdate + "')  AND " + ppm_sql + ")"
                                        + "               AND ppmidx not IN ( select ppmidx from preset_scm_safe_sale_date where  storeno = '" + storeno + "'   and (qty_jungga <= 7 and  qty_safe <=3)  )    " //기준 안전재고, 정가판매 수량에 포함된 물품 제외
                                       + "                 GROUP BY storeno, ppmidx, orderdate"
                                       + "             ) as tbl"
                                       + "             INNER JOIN pos_erp_goods_group as tbl2 ON tbl.product_ppmidx = tbl2.product_ppmidx"
                                       + "             WHERE ISNULL(tbl2.sdate, '') <= '" + orderdate + "' AND (ISNULL(tbl2.edate, '') = '' OR ISNULL(tbl2.edate, '') >= '" + orderdate + "')"
                                       + "             GROUP BY tbl.storeno, tbl2.group_cd, tbl.orderdate"
                                       + "     ) as t1"
                                   // + "     WHERE NOT EXISTS (SELECT * FROM #preset_data_7day WHERE sdate = '" + orderdate + "' AND t1.storeno = storeno AND t1.group_cd = ppmidx)"
                                    //   + "     WHERE NOT EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND t1.storeno = storeno AND t1.group_cd = ppmidx)"
                                     //   + " " + G_sql + " "  //명절 제외 요청
                                       + "     GROUP BY t1.storeno, t1.group_cd"
                                       + "     HAVING ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END,0) <> 0    "
                                       + "     ORDER BY t1.storeno, t1.group_cd";
                                command.Connection = myConnection;
                                command.CommandText = tmp;
                                command.CommandTimeout = 120;
                                command.ExecuteNonQuery();

                                //4주 판매량 평균
                                //sql = "INSERT INTO preset_sale_data (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate)"
                                //sql = "INSERT INTO "+ teble_sale_data + " (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate)"
                                sql = "INSERT INTO #preset_data_4week (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate)"
                                        + "     SELECT '" + orderdate + "', t1.storeno, t1.group_cd, "
                                        + "    ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0),"   //최대값 재외
                                        + "    ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END, 0), getdate()"
                                        + "     FROM ("
                                        + "             SELECT tbl.storeno, tbl2.group_cd, tbl.orderdate, SUM(tbl.qty_total) as qty_total FROM ("
                                        //그룹내 통합코드 물품들의 4주간 정가판매량
                                        + "                 SELECT b.storeno, a.ppmidx_rep as product_ppmidx, b.orderdate, "
                                        // +"                  (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.4 as qty_total "
                                        + "               " + scm_table_w4 + "  "  //4주 평균  *40%
                                        + "                 FROM pos_erp_goods_rep as a"
                                        //+ "                 INNER JOIN pos_sale_A01_ppm_daily as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                        + "                 INNER JOIN "+ scm_table + " as b with(nolock)  ON a.ppmidx_fg = b.ppmidx "
                                        + "                 WHERE orderdate in ('" + date_d_w4 + "','" + date_d_w3 + "','" + date_d_w2 + "','" + date_d_w1 + "')"
                                        + "             " + pps_sql2 + "  "// 테스트 매장과 물품들 쿼리
                                        + "              " + G_pps_sql3 + "" //그룹매장은 카테고리 조건이 없어서 추가
                                        + "                 AND a.rep_g_code IN (SELECT product_cd FROM pos_erp_goods_group WHERE ISNULL(sdate, '') <= '" + orderdate + "' AND (ISNULL(edate, '') = '' OR ISNULL(edate, '') >= '" + orderdate + "'))"
                                        + "                 AND a.del_yn = 'N' AND a.p_ab = 'A'"
                                        + "                 GROUP BY b.orderdate,  a.ppmidx_rep, b.storeno"

                                        + "                 UNION ALL"
                                        //그룹내 일반물품들의 4주간 정가판매량
                                        + "                 SELECT storeno, ppmidx as product_ppmidx, orderdate,"
                                         //+ "                 (SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) * 0.4 as qty_total "
                                        + "               " + scm_table_w4 + "  "  //4주 평균  *40%
                                        // + "                 FROM pos_sale_A01_ppm_daily as a with (nolock)"
                                        + "                 FROM  " + scm_table + "   as a with (nolock)"
                                        + "                 WHERE orderdate in ('" + date_d_w4 + "','" + date_d_w3 + "','" + date_d_w2 + "','" + date_d_w1 + "')"
                                        + "             " + pps_sql2 + "  "// 테스트 매장과 물품들 쿼리
                                        + "                 AND NOT EXISTS (SELECT * FROM pos_erp_goods_rep WHERE (ppmidx_rep = a.ppmidx OR ppmidx_fg = a.ppmidx) AND del_yn = 'N' AND p_ab = 'A')"
                                        + "                 AND ppmidx IN (SELECT b.idx FROM pos_erp_goods_group as a INNER JOIN pos_product_master as b ON a.product_cd = b.icoop_code WHERE ISNULL(a.sdate, '') <= '" + orderdate + "' AND (ISNULL(a.edate, '') = '' OR ISNULL(a.edate, '') >= '" + orderdate + "') AND " + ppm_sql + ")"
                                        + "                 AND ppmidx not IN ( select ppmidx from preset_scm_safe_sale_date where storeno = '" + storeno + "'  and (qty_jungga <= 7 and  qty_safe <=3)   )    "     //기준 안전재고, 정가판매 수량에 포함된 물품 제외
                                        + "                 GROUP BY storeno, ppmidx, orderdate"
                                        + "             ) as tbl"
                                        + "             INNER JOIN pos_erp_goods_group as tbl2 ON tbl.product_ppmidx = tbl2.product_ppmidx"
                                        + "             WHERE ISNULL(tbl2.sdate, '') <= '" + orderdate + "' AND (ISNULL(tbl2.edate, '') = '' OR ISNULL(tbl2.edate, '') >= '" + orderdate + "')"
                                        + "             GROUP BY tbl.storeno, tbl2.group_cd, tbl.orderdate"
                                        + "     ) as t1"
                                     //   + "     WHERE NOT EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND t1.storeno = storeno AND t1.group_cd = ppmidx)"
                                        // + "     WHERE NOT EXISTS (SELECT * FROM #preset_data_4week  WHERE sdate = '" + orderdate + "' AND t1.storeno = storeno AND t1.group_cd = ppmidx)"
                                        //+ " " + G_sql + " "  //명절 제외 요청
                                        + "     GROUP BY t1.storeno, t1.group_cd"
                                        + "     HAVING ISNULL(CASE WHEN (COUNT(t1.orderdate)) <> 0 THEN ROUND(SUM(t1.qty_total)/COUNT(t1.orderdate), 1) ELSE 0 END,0) <> 0    "
                                        + "     ORDER BY t1.storeno, t1.group_cd";
                            }
                            command.Connection = myConnection;
                            command.CommandText = sql;
                            command.CommandTimeout = 120;
                            command.ExecuteNonQuery();

                            // 7일간 임시테이블에 4주간 데이터 insert, update
                            sql = " merge into #preset_data_7day as T "
                                       + " using ( "
                                       //+ " select sdate, storeno, ppmidx, amt_sale_first, amt_sale from  #preset_data_7day where sdate ='" + orderdate + "' " + pps_sql +"  "
                                       + " select sdate, storeno, ppmidx, amt_sale_first, amt_sale from  #preset_data_4week where sdate ='" + orderdate + "' " + pps_sql + "  "
                                       + "    ) AS S "
                                       + "  on t.sdate = s.sdate and t.storeno = s.storeno and t.ppmidx = s.ppmidx "
                                       + "  when not matched then "
                                       + " 	insert (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate, finalAdmin) values ('" + orderdate + "', s.storeno, s.ppmidx ,  s.amt_sale_first , s.amt_sale,getdate(),'I') "
                                       + "   WHEN MATCHED THEN "
                                       + " update set t.amt_sale_first = t.amt_sale_first + S.amt_sale_first , t.amt_sale = t.amt_sale + S.amt_sale ,t.finalAdmin ='U';";
                            command.Connection = myConnection;
                            command.CommandText = sql;
                            command.CommandTimeout = 120;
                            command.ExecuteNonQuery();

                            // insert, update한 데이터를 실테이블 판매예측값에 적용
                            //sql = " INSERT INTO " + teble_sale_data + " (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate,finalAdmin)   "
                            //            + " select  sdate, storeno, ppmidx, ROUND(amt_sale_first,0) , ROUND(amt_sale,0),getdate(), 'DEMON_2' from #preset_data_7day where  sdate ='" + orderdate + "' " + pps_sql + " and ROUND(amt_sale_first,0) > 0 ";
                            //command.Connection = myConnection;
                            //command.CommandText = sql;
                            //command.CommandTimeout = 120;
                            //command.ExecuteNonQuery();

                        }

                    }
                    catch (Exception ex)
                    {
                        frm1.sendSms("[시뮬레이션 조건]오류:" + ex.Message);
                        frm1.Log("[시뮬레이션]오류:" + ex.Message);
                    }

                    try
                    {
                        //4. [판매예측값<0]인 경우 판매예측값=0으로 조정
                        //  sql = "IF EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND amt_sale < 0)"
                        sql = " INSERT INTO " + teble_sale_data + " (sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate,finalAdmin)   "
                                            + " select  sdate, storeno, ppmidx, ROUND(amt_sale_first,0) , ROUND(amt_sale,0),getdate(), 'DEMON_2' from #preset_data_7day where  ROUND(amt_sale_first,0) > 0 ";
                        command.Connection = myConnection;
                        command.CommandText = sql;
                        command.CommandTimeout = 120;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        frm1.sendSms("[정가판매값생성_v3(insert)]:" + ex.Message);
                        frm1.Log("[정가판매값생성_v3(#preset_data_7day)]:" + ex.Message);
                    }

                  //  frm1.Log("[" +date_d_d1+"]");
                    

                }
                catch (Exception ex)
                {
                  
                    frm1.sendSms("[판매예측값생성1_v3]:" + ex.Message);
                    frm1.Log("[판매예측값생성1_v3(fn_set_preset_sale_data)]:" + ex.Message);
                }

               

                try
                {
                    //그룹별 판매예측값 생성 후 문자전송
                    if (gubun.Equals("G"))
                    {
                        try
                        {
                            //4. [판매예측값<0]인 경우 판매예측값=0으로 조정
                            //  sql = "IF EXISTS (SELECT * FROM preset_sale_data WHERE sdate = '" + orderdate + "' AND amt_sale < 0)"
                            sql = "IF EXISTS (SELECT * FROM " + teble_sale_data + " WHERE sdate = '" + orderdate + "' AND amt_sale < 0)"
                                     + " BEGIN"
                                    //      + " UPDATE preset_sale_data"
                                    + " UPDATE " + teble_sale_data + ""
                                    + " SET amt_sale_first = 0, amt_sale = 0"
                                    + " WHERE sdate = '" + orderdate + "' AND amt_sale < 0"
                                    + " END";
                            command.Connection = myConnection;
                            command.CommandText = sql;
                            command.CommandTimeout = 120;
                            command.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            frm1.sendSms("[판매예측값생성2_v3]:" + ex.Message);
                            frm1.Log("[판매예측값생성2_v3(preset_sale_data)]:" + ex.Message);
                          
                        }

                        //5. 생성된 판매예측값 내역 갯수 체크
                        //sql = "SELECT COUNT(*) as cnt FROM preset_sale_data WHERE sdate = '" + orderdate + "'and finalAdmin in ('I','U') ";
                        sql = "SELECT COUNT(*) as cnt FROM "+teble_sale_data+" WHERE sdate = '" + orderdate + "'and finalAdmin ='DEMON_2' ";
                        command.CommandText = sql;
                        SqlDataReader rd2 = command.ExecuteReader();
                        while (rd2.Read())
                        {
                            cnt = int.Parse(rd2["cnt"].ToString());
                        }
                        rd2.Close();

                        if (teble_sale_data == "preset_sale_data")
                        {

                            //신규로 생성된 판매예측 정보 저장
                            sql = " delete preset_sale_data_v2 "
                                    + " where sdate = '" + orderdate + "'   ";
                            command.Connection = myConnection;
                            command.CommandText = sql;
                            command.CommandTimeout = 120;
                            command.ExecuteNonQuery();

                            //신규로 생성된 판매예측 정보 저장
                            sql = " insert into preset_sale_data_v2(sdate, storeno, ppmidx, amt_sale_first, amt_sale, regdate) "
                                    + "  select  sdate, storeno, ppmidx, amt_sale_first, amt_sale,regdate from preset_sale_data  where sdate = '" + orderdate + "' and finalAdmin = 'DEMON_2'  ";
                            command.Connection = myConnection;
                            command.CommandText = sql;
                            command.CommandTimeout = 120;
                            command.ExecuteNonQuery();

                        }

                        //if (pspd_orderdate != "" ) //신규테이블에 자료값이 있으면 30일 자료 삭제
                        //{
                        //    sql = " delete preset_sale_ppm_daily where orderdate = '" + date_d_del + "'  ";
                        //    command.Connection = myConnection;
                        //    command.CommandText = sql;
                        //    command.CommandTimeout = 120;
                        //    command.ExecuteNonQuery(); 
                        //}


                        //  4 - 1.판매예측값 특이값 제외 및 치환(1보다 작을 경우 1로 변경, 1보다 클경우 반올림) preset_sale_data.amt_sale : 최종 판매예측값으로 업데이트
                        sql = "   update  a   "
                                + " set a.amt_sale = ( "
                                + " case   "
                                + " when s1.avg_jungga_3 < 1 then CEILING(s1.avg_jungga_3)   "
                                + " when  s1.avg_jungga_3 >= 1 then   Round(s1.avg_jungga_3,0) else 0 end )   "
                                + "  from preset_sale_data a    "
                                + "   left join  "
                                + " (  "
                                + " select storeno, ppmidx, avg_jungga_3 , except_4 from preset_safe_stdevp_total  "
                                + " ) as s1 on a.storeno = s1.storeno and a.ppmidx = s1.ppmidx  "
                                + "   where   a.sdate ='" + orderdate + "' and a.storeno in ('6012','6085','6223') and a.ppmidx in (select CONVERT(varchar, idx)  from  pos_product_master where pcmidx in ('59' ,'32' ,'34','51')) and s1.except_4 < a.amt_sale_first ";
                        command.Connection = myConnection;
                        command.CommandText = sql;
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();



                        frm1.sendSms("[" + orderdate + "] D-4 판매예측값 " + (cnt).ToString() + "건 생성완료(" + String.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now.ToString()) + ") "+ scm_table + "");
                        frm1.Log("[STEP2.판매예측값생성(fn_set_preset_sale_data)]: " + (cnt).ToString() + "건 생성완료");
                    }
                     
                }
                catch (Exception ex)
                {
                    frm1.sendSms("[판매예측값생성4_v3]:" + ex.Message);
                    frm1.Log("[판매예측값생성4_v3(fn_set_preset_sale_data)]:" + ex.Message);
                }

            }
            else
            {
                frm1.sendSms("[판매예측값생성2_v3]:orderdate 없음");
                frm1.Log("[판매예측값생성2_v3(fn_set_preset_sale_data)]:orderdate 없음");
            }

            myConnection.Dispose();
            myConnection.Close();

        }
        #endregion

        #region 그룹물품 1팀 추출
        private void fn_get_preset_group_data(string orderdate, Form1 frm1)
        {
            //그룹물품 추출 프로세스
            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("164");
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            if (!orderdate.Equals(""))
            {
                String sql = null;
                MySqlDataAdapter adapter_my = new MySqlDataAdapter();
                SqlDataAdapter adapter = new SqlDataAdapter();
                SqlCommand command = new SqlCommand();
                DataSet ds = new DataSet();

                String orderdate2 = orderdate.Replace("-", "/");

                try
                {
                    //2. 프로시저 호출
                    //D-6, D-3 실행하므로 기존에 있는 물품인 경우, 설정값 Update 없는 물품인 경우 Insert
                    sql = "IF EXISTS (SELECT * FROM pos_erp_goods_group)"
                            + " BEGIN "
                            + "     DELETE FROM pos_erp_goods_group"
                            + " END"
                            + " INSERT INTO pos_erp_goods_group (product_cd, product_ppmidx, group_cd, group_nm, product_prio, sdate, edate, regdate)"
                            + " SELECT g_code, b.idx, altergr_code, altergr_name, priority, "
                            + " CASE WHEN a.fs_day = '' THEN null ELSE replace(ISNULL(a.fs_day,' '), '/','-') END, CASE WHEN a.ts_day = '' THEN null ELSE replace(ISNULL(a.ts_day,' '), '/','-') END, getdate()"
                            + " FROM openquery (LINK_COOPBASE, 'SELECT g_code, altergr_code, altergr_name, priority, fs_day, ts_day FROM vw_alternativegroups') as a"
                            + " INNER JOIN pos_product_master as b ON a.g_code = b.icoop_code"
                            //임시로 여기서 거름 (pos_erp_goods_rep 에서...)
                            + " WHERE NOT EXISTS (SELECT * FROM pos_erp_goods_rep WHERE del_yn ='N' AND p_ab = 'A' AND a.g_code = fg_code)"
                            + " ORDER BY altergr_code, priority";
                    //★★★★★★★★★실서버는 vw_alternativegroups  테스트서버는 vw_alternativegroups3
                    command.Connection = myConnection;
                    command.CommandText = sql;
                    command.CommandTimeout = 120;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    frm1.sendSms("[그룹물품추출1_v3(fn_get_preset_group_data)]오류:" + ex.Message);
                    frm1.Log("[그룹물품추출1_v3(fn_get_preset_group_data)]오류:" + ex.Message);

                }
            }
            else
            {
                frm1.sendSms("[그룹물품추출2_v3(fn_get_preset_group_data)]오류:orderdate 없음");
                frm1.Log("[그룹물품추출2_v3(fn_get_preset_group_data)]오류:orderdate 없음");
            }

            frm1.Log("[그룹물품추출_v3(fn_get_preset_group_data)]완료");

            MYconn.Dispose();
            myConnection.Dispose();

            MYconn.Close();
            myConnection.Close();

        }
        #endregion

    }

}

