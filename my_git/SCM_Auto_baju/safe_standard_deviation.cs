using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Windows.Forms;
using System.Collections;

namespace SCM_Auto_baju
{
    class safe_standard_deviation
    {
        SqlConnection myConnection = new SqlConnection();
        MySqlConnection MYconn = new MySqlConnection();

        public void fn_safe_standard_deviation(string safe_sale_sdate, string safe_sale_edate, string except, string gubun, Form1 frm1)
        {

            DateTime today = DateTime.Now.Date; //현재 날짜 확인
            var ex_sdate = today.AddMonths(0); // 월 구함

            //매년 01월01일 연간,월간 안전재고 생성시 ABC누적등급을 한번만 실행
            string today_chek = string.Format(ex_sdate.AddDays(0).ToString("MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경 (01-01일 확인하기 위하여)

            string ex = "";

            //if (gubun == "Y") //연간 정가 판매율일 경우
            //{
            //    ex = "연간";
            //    frm1.sendSms("" + ex + " 표준편차 데이터 생성 시작");
            //    fn_safe_standard_deviation_ppm_year(safe_sale_sdate, safe_sale_edate, frm1); //작년도 정가판매량 자료 백업
            //    frm1.sendSms("" + ex + " 정가판매량 생성 끝");
            //}
            //else
            //{
            //    ex = "월간";
            //    frm1.sendSms("" + ex + " 표준편차 데이터 생성 시작");
            //}

            //fn_safe_standard_deviation_delete(gubun, frm1); //기존 생성된 표준편차 삭제
            //frm1.sendSms(" " + ex + " 0. 이전 표준편차 삭제 끝");
            ////fn_safe_standard_deviation_total(safe_sale_sdate, safe_sale_edate, gubun, frm1); //1단계 표준편차 자료 생성
            ////frm1.sendSms(" " + ex + " 1-1. 표준편차 1차 생성 끝");
            ////fn_safe_standard_deviation_except(except, gubun, frm1); //2단계 정가판매수량 특이값 제외 생성
            ////frm1.sendSms("" + ex + " 특이값제외 2차 생성 끝");
            ////fn_safe_standard_deviation_total_3_1(safe_sale_sdate, safe_sale_edate, gubun, frm1); //3-1단계 특이값제외 표준편차
            ////frm1.sendSms("" + ex + " 1-2. 표준편차 3-1차 생성 끝");
            ////fn_safe_standard_deviation_weekday_3_2(safe_sale_sdate, safe_sale_edate, gubun, frm1); //3-2단계 요일별 표준편차
            ////frm1.sendSms("" + ex + " 요일별 표준편차 3-2차 생성 끝");

            //if (gubun == "M" && today_chek != "01-01") //매년 01-01일에 ABC 누적등급은 한번만 실행하기 위해서
            //{

            //    fn_get_safe_standard_deviation_log(frm1);     //전달 ABC 누적등급 백업
            //    frm1.sendSms("" + ex + " ABC 누적등급  4차 백업 끝");
            //    fn_get_safe_standard_deviation_total(safe_sale_sdate, safe_sale_edate, gubun, frm1);  //월간 ABC 누적등급 생성
            //    //frm1.sendSms("" + ex + " ABC 누적등급 4차 생성 끝");
            //    frm1.sendSms("" + safe_sale_sdate + "~" + safe_sale_edate + " " + ex + " ABC 누적등급 4차 생성 끝");

            //}
            //else if (gubun == "Y")
            //{
            //    //매년은 당일기준 12-01~12-31로 실행하여야 됨
            DateTime today_3 = DateTime.Now.Date; //현재 날짜 확인
            var ex_today_3 = today_3.AddMonths(-1); // 전월 구함
            var ex_sdate_3 = ex_today_3.AddDays(1 - today_3.Day); //전월의 날짜 1일 구함

            var ex_etoday_3 = today_3.AddMonths(0); // 당월 구함
            var ex_edate_3 = ex_etoday_3.AddDays(-1);//금일에 -1일
            string sdate_3 = string.Format(ex_sdate_3.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경
            string edate_3 = string.Format(ex_edate_3.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경

            //    fn_get_safe_standard_deviation_log(frm1);     //전달 ABC 누적등급 백업
            //frm1.sendSms("" + ex + " ABC 누적등급  4차 백업 끝");
            fn_get_safe_standard_deviation_total(sdate_3, edate_3, gubun, frm1);  //월간 ABC 누적등급 생성
            frm1.sendSms("" + safe_sale_sdate + "~" + safe_sale_edate + " " + ex + " ABC 누적등급 4차 생성 끝");
            //}

            //fn_safe_standard_deviation_except_2(except, gubun, frm1); //4-1단계 정가판매수량 특이값 제외 생성
            //frm1.sendSms("" + ex + "  특이값제외 4-1차 생성 끝");
            //fn_get_safe_calculate(gubun, frm1);
            //frm1.sendSms("" + ex + " 안전재고 수량 산출 4-2차 생성 끝");
            //안전재고 주석 제거!!!!!
            frm1.sendSms("" + ex + " 표준편차 데이터 생성 종료");
        }

        private void fn_safe_standard_deviation_total(string safe_sale_sdate, string safe_sale_edate, string gubun,  Form1 frm1)
        {

            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("42");
            MySqlDataAdapter adapter_my = new MySqlDataAdapter();
            MySqlDataAdapter adapter1_my = new MySqlDataAdapter();

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlDataAdapter adapter = new SqlDataAdapter();
            SqlDataAdapter adapter1 = new SqlDataAdapter();

            string day1 = "";
            string table_name = "";

            if (gubun == "Y") //연간 표준편차
            {
                day1 = " >=365";
                table_name = "preset_sale_ppm_daily_year";
            }
            else
            {
                day1 = " < 365";
                table_name = "preset_sale_ppm_daily";
            }

            //매장별, 물품별 표준편차를 구하기 위해 매장, 물품정보 추출
            // 제외 카테고리 포함
            string queryString = "select  a.storeno, a.ppmidx from pos_product_date as a with (nolock) "
            + " left join pos_product_master as b with(nolock) on a.ppmidx = b.idx "
            + " left join pos_db_BrandClass as c with (nolock) on b.pdbidx = c.idx "
            + " where c.idx not in ('3','4','5','6','8','10','12','13','14','15','16','17') and  "
            // + " b.pcmidx  in (select idx from  pos_category_master where idx not in ('77','117','169','23','107','1','164','155','18','102','85','15','4','103','129','108')) "
            + " b.pcmidx  in (select idx from  pos_category_master where idx not in ('1','4','15','18','23','62','77','83','84','85','86','102','107','106','108','109','155','103','129','130','144','161','164') and pcgidx not in('32','33')) "
            // + " and a.storeno <> '' and a.ppmidx <> '' and  DATEDIFF ( day , a.sdate_input , '" + safe_sale_edate + "') >= 365 and sdate_input is not null " 
            + " and a.storeno <> '' and a.ppmidx <> '' and  DATEDIFF ( day , a.sdate_input , getdate()) " + day1 + " and sdate_input is not null "
            + " and b.erp_p_ab = 'A' and  b.erp_recall_ab  <> 'A' and  b.erp_shop_release_date > 1 "  //공급구분: A, 재고구분 <>A, 출고기한 >1
            + " and a.storeno in (select RIGHT(storeno,4) as storeno from pos_store where storeType ='B' and vc_businessStatus ='1')  "  //직영매장만 적용
            + " order by storeno, ppmidx ";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            SqlCommand command = new SqlCommand(queryString, myConnection);

            try
           {
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                double sale_count = 0d;


                string storeno_check = "";


                foreach (DataRow dr in rows)
                {
                    double sum_qty = 0d;

                    storeno = dr["storeno"].ToString();
                    ppmidx = dr["ppmidx"].ToString();

                    if (storeno_check != storeno) //매장코드가 다르면
                    {
                         sale_count = 0;

                        //매장영업 일수를 카운트
                        string queryString2 = " select  count(idx) as sale_count from  pos_sale_A01  with (nolock) "
                                       + " where (orderdate >= '" + safe_sale_sdate + "' and orderdate <=  '" + safe_sale_edate + "') and storeno = '800000" + storeno + "'  ";

                        SqlCommand command2 = new SqlCommand(queryString2, myConnection);
                        SqlDataReader reader2 = command2.ExecuteReader();
                        while (reader2.Read())
                        {
                            sale_count = Convert.ToDouble(reader2["sale_count"].ToString());  //매장 영업 횟수
                        }
                        reader2.Close();
                    }

                    //매장의 물품의 총정가판매량 합계 
                    string queryString3 = " select   ifnull(sum(qty_jungga),0)  as sum_jungga from "+ table_name + "    "
                         + " where  storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and (orderdate >= '2018-01-01' and orderdate <= '2018-12-31')  ";  //정가판매 수량
                                                                                                                                                                                              //  + " where  storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and (orderdate >= '" + safe_sale_sdate + "' and orderdate <= '" + safe_sale_edate + "')  ";  //정가판매 수량
                                                                                                                                                                                              //string queryString3 = "select sum_jungga from preset_sale_ppm_jungga_total  where storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  ";

                    MySqlCommand command3 = new MySqlCommand(queryString3, MYconn);
                    MySqlDataReader reader3 = command3.ExecuteReader();
                    //SqlCommand command3 = new SqlCommand(queryString3, myConnection);
                    //SqlDataReader reader3 = command3.ExecuteReader();
                    while (reader3.Read())
                    {
                        sum_qty = Convert.ToDouble(reader3["sum_jungga"].ToString());
                    }
                    reader3.Close();

                    if (sum_qty != 0)
                    {
                        //배열에 입력될 날짜별 정가판매량계산 
                        string queryString1 = "   select  orderdate,  storeno, ppmidx, sum(qty_jungga) as qty_jungga   from   " + table_name + "   " //preset_sale_ppm_daily_year
                             + "   WHERE storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and   (orderdate >= '2018-01-01' and orderdate <= '2018-12-31')  "

                           //  + "   WHERE storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and   (orderdate >=  '" + safe_sale_sdate + "' and orderdate <=  '" + safe_sale_edate + "')  "
                                                                    // + "   WHERE  (orderdate >='2018-01-01' and orderdate <= '2018-12-31') and storeno = '" + storeno.Substring(6, 4) + "'  AND   ppmidx = '" + ppmidx + "' "
                                                                    // + " where storeno ='8000006096' and  ppmidx ='10139'  "
                                                                    + " group by orderdate, storeno, ppmidx "
                                                                    + " order by orderdate ";

                    
                        adapter1_my.SelectCommand = new MySqlCommand(queryString1, MYconn);
                        MySqlCommand command1 = new MySqlCommand(queryString1, MYconn);

                        //adapter1.SelectCommand = new SqlCommand(queryString1, myConnection);
                        //SqlCommand command1 = new SqlCommand(queryString1, myConnection);
                        try
                        {

                            DataSet ds1 = new DataSet();
                            adapter1_my.Fill(ds1);
                            DataTable table1 = ds1.Tables[0];
                            DataRowCollection rows1 = table1.Rows;

                            int qty_jungga = 0;
                            int check_count = 0;
         

                            List<int> list = new List<int>();

                            foreach (DataRow dr1 in rows1) //조회된 데이터만큼 루프
                            {
                                qty_jungga = Convert.ToInt32(dr1["qty_jungga"].ToString());
                                //storeno_1 = dr1["storeno"].ToString();

                                list.Add(qty_jungga); //배열에 정가판매량 입력

                                check_count += 1;
                            }

                            for (int i = check_count; i < sale_count; i++)  //영업일수만큼 판매량이 있어야되기 때문에 조회된 데이터외의 값은 0으로 입력(중요!)
                            {
                                list.Add(0);
                            }

                            double average = sum_qty / sale_count;   //정가판매수량/영업일수
                            int valueCount = list.Count; //배열에 입력된 갯수

                            if (valueCount == 0)
                            {
                                frm1.Log("[배열에 입력된 값없음]:" + valueCount);
                            }


                            double standardDeviation = 0d;
                            double variance = 0d;

                            try
                            {
                                for (int i = 0; i < valueCount; i++)
                                {
                                    variance += Math.Pow(list[i] - average, 2); //제곱근 함수 
                                }
                                standardDeviation = Math.Sqrt(SafeDivide(variance, valueCount)); //루트 함수 (제곱근 합계와 배열의 갯수 나눈값)

                                //queryString1 = "IF EXISTS (SELECT * FROM preset_safe_stdevp_total WITH (NOLOCK) WHERE storeno = '" + storeno+ "' AND ppmidx ='" + ppmidx + "' )"
                                //                      + " BEGIN "
                                //                      + "     UPDATE preset_safe_stdevp_total "
                                //                      + "     SET stdevp_1 =  " + Math.Round(standardDeviation, 2) + " "
                                //                      + "    , avg_jungga_1 =" + Math.Round(average, 2) + ",regdate = getdate()  "
                                //                      + "      WHERE storeno = '" + storeno + "' AND ppmidx ='" + ppmidx + "' "
                                //                      + " END"
                                //                      + " ELSE"
                                //                      + " BEGIN"
                                //                      + "     insert into preset_safe_stdevp_total "
                                //                      + "      (storeno,ppmidx,stdevp_1,avg_jungga_1)  "
                                //                      + "     values ('" + storeno + "' ,'" + ppmidx + "' ," + Math.Round(standardDeviation, 2) + ","+ Math.Round(average,2) +") "
                                //                      + " END";

                                queryString1 = "insert into preset_safe_stdevp_total(storeno,ppmidx,stdevp_1,avg_jungga_1,gubun)  values "
                                                                + " ('" + storeno + "', '" + ppmidx + "', " + Math.Round(standardDeviation, 2) + ", "+ Math.Round(average,2) +", '" + gubun + "' )";
                                //Log((String.Format("[in_other_log] 쿼리[{0}] : ", queryString1)));
                                command1.CommandText = queryString1;
                                command1.ExecuteNonQuery();

                                list.Clear(); //배열 초기화
                            }
                            catch (Exception)
                            {
                                throw;
                            }
                        }
                        catch (Exception ex)
                        {
                            frm1.Log("[정가 판매 표준 편차]:" + ex.Message);
                            frm1.Log("정가 표준 편차쿼리:" + queryString1);

                        }
                    }

                    storeno_check = storeno; //매장코드 체크
                } //for()
            }
            catch (Exception ex)
            {
                frm1.Log("[정가 판매 표준 편차 pos_store_stock]:" + ex.Message);
                frm1.sendSms("[정가 판매 표준 편차 pos_store_stock]:" + ex.Message);

            }
            frm1.Log("[1차 정가 판매 표준 편차 완료]");
            myConnection.Dispose();
            myConnection.Close();
        }
        private double SafeDivide(double value1, double value2) //모든 표준편차 나누기 함수
        {
            double result = 0d;

            try
            {
                if ((value1 == 0) || (value2 == 0))
                {
                    return 0d;
                }
                result = value1 / value2;
            }
            catch
            {

            }
            return result;
        }
        private void fn_safe_standard_deviation_except(string except, string gubun, Form1 frm1) //2단계 정가판매수량 특이값 제외 수량
        {
            string sql = null;

            //myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            // SqlCommand command = new SqlCommand(sql, myConnection);
            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("42");
            MySqlCommand command = new MySqlCommand(sql, MYconn);

            try
            {
                //string queryString1 = " update  a "
                //                                     + " set a.except_2 = ifnull(s1.except_2,0) "
                //                                     + "  from preset_safe_stdevp_total a "
                //                                     + "  left join "
                //                                     + "   ( "
                //                                     + " select b.storeno, b.ppmidx, Round(avg_jungga_1 + (b.stdevp_1 * " + except + "),0)  as except_2 from  "
                //                                     + " preset_safe_stdevp_total as b where gubun = '" + gubun + "'  "
                //                                     + "  group by b.storeno,  b.ppmidx,stdevp_1, avg_jungga_1 "
                //                                     + "    ) as s1 on a.storeno = s1.storeno and a.ppmidx = s1.ppmidx  ";


                string queryString1 = " UPDATE preset_safe_stdevp_total a "
                                            + "LEFT JOIN( "
                                            + "SELECT b.storeno, b.ppmidx, ROUND(avg_jungga_1 + (b.stdevp_1 * 2.58), 0)  AS except_2 FROM "
                                            + "preset_safe_stdevp_total AS b WHERE gubun = 'Y' "
                                            + "GROUP BY b.storeno, b.ppmidx, stdevp_1, avg_jungga_1 "
                                            + ") AS s1 ON a.storeno = s1.storeno AND a.ppmidx = s1.ppmidx "
                                            + "SET a.except_2 = IFNULL(s1.except_2, 0) ";

                command.CommandText = queryString1;
                command.ExecuteNonQuery();

                frm1.Log("[2차 정가판매수량 특이값 제외 완료]");
            }
            catch(Exception ex)
            {
                frm1.Log("[2단계 정가판매수량 특이값 제외 수량 preset_safe_stdevp_total]:" + ex.Message);
                frm1.sendSms("[2단계 정가판매수량 특이값 제외 수량 preset_safe_stdevp_total]:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();

        }

        private void fn_safe_standard_deviation_except_2(string except, string gubun, Form1 frm1) //4-1단계 정가판매수량 특이값 제외 수량
        {
            string sql = null;

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlCommand command = new SqlCommand(sql, myConnection);
            try
            {

                string queryString1 = " update  a "
                                                     + " set a.except_4 =  isnull(s1.except_4, 0) "
                                                     + "  from preset_safe_stdevp_total a "
                                                     + "  left join "
                                                     + "   ( "
                                                     + " select b.storeno, b.ppmidx, Round(avg_jungga_3 + (b.stdevp_3 * " + except + "),0)  as except_4 from  "
                                                     + " preset_safe_stdevp_total as b with (nolock) where gubun ='" + gubun + "'  "
                                                     + "  group by b.storeno,  b.ppmidx,stdevp_3, avg_jungga_3 "
                                                     + "    ) as s1 on a.storeno = s1.storeno and a.ppmidx = s1.ppmidx  ";

                command.CommandText = queryString1;
                command.ExecuteNonQuery();

                frm1.Log("[4-1차 정가판매수량 특이값 제외 완료]");
            }
            catch(Exception ex)
            {
                frm1.Log("[4-1차 정가판매수량 특이값 제외 수량 preset_safe_stdevp_total]:" + ex.Message);
                frm1.sendSms("[4-1차 정가판매수량 특이값 제외 수량 preset_safe_stdevp_total]:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();

        }

        private void fn_safe_standard_deviation_total_3_1(string safe_sale_sdate, string safe_sale_edate, string gubun, Form1 frm1) //3-1단계 2단계 제외 수량을 제외한 표준편차
        {

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            SqlDataAdapter adapter = new SqlDataAdapter();
            SqlDataAdapter adapter1 = new SqlDataAdapter();

            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("42");
            MySqlDataAdapter adapter_my = new MySqlDataAdapter();
            MySqlDataAdapter adapter1_my = new MySqlDataAdapter();


            string table_name = "";

            if (gubun == "Y") //연간 표준편차
            {
                table_name = "preset_sale_ppm_daily_year";
            }
            else
            {

                table_name = "preset_sale_ppm_daily";
            }

            string queryString = " select storeno, ppmidx from preset_safe_stdevp_total where gubun ='" + gubun + "'  order by storeno, ppmidx  ";
            adapter_my.SelectCommand = new MySqlCommand(queryString, MYconn);
            MySqlCommand command = new MySqlCommand(queryString, MYconn);

            try
            {
                DataSet ds = new DataSet();
                adapter_my.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                double minus_count = 0d;
                double sale_count = 0d;
                double sale_count_fianl = 0d;
                string storeno_check = "";
                int except_2 = 0;


                foreach (DataRow dr in rows)
                {
                    double sum_qty = 0d;

                    storeno = dr["storeno"].ToString();
                    ppmidx = dr["ppmidx"].ToString();

              
                    if (storeno_check != storeno) //매장코드가 다르면
                    {
                        sale_count = 0;

                        //매장영업 일수를 카운트
                        string queryString2 = " select  count(idx) as sale_count from  pos_sale_A01  with (nolock) "
                                       + " where (orderdate >= '" + safe_sale_sdate + "' and orderdate <=  '" + safe_sale_edate + "') and right(storeno,4) = '" + storeno + "' ";

                        SqlCommand command2 = new SqlCommand(queryString2, myConnection);
                        SqlDataReader reader2 = command2.ExecuteReader();
                        while (reader2.Read())
                        {
                            sale_count = Convert.ToDouble(reader2["sale_count"].ToString());  //매장 영업 횟수
                        }
                        reader2.Close();
                    }
                    string queryString4 = " select except_2  from "
                                                            + "preset_safe_stdevp_total  "
                                                           + " where  storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and gubun ='"+gubun+"' ";  //특이값 제외
                    //string queryString3 = "select sum_jungga from preset_sale_ppm_jungga_total  where storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  ";

                    MySqlCommand command4 = new MySqlCommand(queryString4, MYconn);
                    MySqlDataReader reader4 = command4.ExecuteReader();
                    while (reader4.Read())
                    {
                        except_2 = Convert.ToInt32(reader4["except_2"].ToString());
                    }
                    reader4.Close();


                    //string queryString3 = " select   isnull(sum(a.qty_jungga),0)  as sum_jungga from "
                    //                                            + "preset_sale_ppm_daily_back3   as a with (nolock)  "
                    //                                            + " left join  preset_safe_stdevp_total as b with (nolock) on a.storeno = b.storeno and b.ppmidx = a.ppmidx "
                    //                                           + " where  a.storeno = '" + storeno.Substring(6, 4) + "'  AND   a.ppmidx = '" + ppmidx + "' and (a.orderdate >= '" + safe_sale_sdate + "' and a.orderdate <= '" + safe_sale_edate + "') and a.qty_jungga <= b.except_2  ";  //정가판매 수량

                    // string queryString3 = "select sum_jungga from preset_sale_ppm_jungga_total  where storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  and qty_jungga <= " + except_2 + "   ";
                   
                    //매장의 물품의 총정가판매량 합계,  특이값제외한 매장일수 계산 (총영업일수 - 특이값제외포함일수)
                    string queryString3 = "select ifnull(sum(qty_jungga),0)  as sum_jungga  " 
                                                            + ", (SELECT count(orderdate)   FROM " + table_name + "   WHERE storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and(orderdate >= '" + safe_sale_sdate + "' and orderdate <= '" + safe_sale_edate + "') and qty_jungga > " + except_2 + ")  as minus_count "
                                                            + "  from " + table_name + " "
                                                            +  "where storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  and (orderdate >= '" + safe_sale_sdate + "' and orderdate <= '" + safe_sale_edate + "') and qty_jungga <= " + except_2 + "   ";
                    MySqlCommand command3 = new MySqlCommand(queryString3, MYconn);
                    MySqlDataReader reader3 = command3.ExecuteReader();
                    while (reader3.Read())
                    {
                        sum_qty = Convert.ToDouble(reader3["sum_jungga"].ToString());
                        minus_count = Convert.ToDouble(reader3["minus_count"].ToString());
                    }
                    reader3.Close();

                  
                    //(총영업일수 - 특이값제외포함일수)
                    sale_count_fianl = sale_count - minus_count;

                    if (sum_qty != 0)
                    {
                     
                        string queryString1 = "  select  orderdate,   storeno, ppmidx, sum(qty_jungga) as qty_jungga   from  "
                                                                  + " "+ table_name + " "
                                                                   + "   where  storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and (orderdate >= '" + safe_sale_sdate + "' and orderdate <= '" + safe_sale_edate + "') and qty_jungga <= "+ except_2 +"   "
                                                                  // + "   WHERE  (orderdate >='2018-01-01' and orderdate <= '2018-12-31') and storeno = '" + storeno.Substring(6, 4) + "'  AND   ppmidx = '" + ppmidx + "' "
                                                                  // + " where storeno ='8000006096' and  ppmidx ='10139'  "
                                                                  + " group by orderdate, storeno, ppmidx "
                                                                  + " order by orderdate ";

                        adapter1_my.SelectCommand = new MySqlCommand(queryString1, MYconn);
                        MySqlCommand command1 = new MySqlCommand(queryString1, MYconn);
                        try
                        {

                            DataSet ds1 = new DataSet();
                            adapter1_my.Fill(ds1);
                            DataTable table1 = ds1.Tables[0];
                            DataRowCollection rows1 = table1.Rows;

                            int qty_jungga = 0;
                            int check_count = 0;

                            List<int> list_2 = new List<int>();

                            foreach (DataRow dr1 in rows1) //조회된 데이터만큼 루프
                            {
                                qty_jungga = Convert.ToInt32(dr1["qty_jungga"].ToString());
                                //storeno_1 = dr1["storeno"].ToString();

                                list_2.Add(qty_jungga); //배열에 정가판매량 입력

                                check_count += 1;
                            }

                            for (int i = check_count; i < sale_count_fianl; i++)  //영업일수만큼 판매량이 있어야되기 때문에 조회된 데이터외의 값은 0으로 입력(중요!)
                            {
                                list_2.Add(0);
                            }

                            double average = sum_qty / sale_count_fianl;   //정가판매수량/영업일수
                            int valueCount = list_2.Count; //배열에 입력된 갯수

                            if (valueCount == 0)
                            {
                                frm1.Log("[배열에 입력된 값없음]:" + valueCount);
                            }


                            double standardDeviation = 0d;
                            double variance = 0d;

                            try
                            {
                                for (int i = 0; i < valueCount; i++)
                                {
                                    variance += Math.Pow(list_2[i] - average, 2); //제곱근 함수 
                                }
                                standardDeviation = Math.Sqrt(SafeDivide(variance, valueCount)); //루트 함수

                                queryString1 = " UPDATE preset_safe_stdevp_total "
                                                           + "SET stdevp_3 = " + Math.Round(standardDeviation, 2) + "  , avg_jungga_3 = " + Math.Round(average,2) + " , regdate =now()"
                                                           + "      WHERE storeno = '" + storeno + "' AND ppmidx ='" + ppmidx + "' and gubun = '" + gubun + "' ";

                                //Log((String.Format("[in_other_log] 쿼리[{0}] : ", queryString1)));
                                command.CommandText = queryString1;
                                command.ExecuteNonQuery();

                                list_2.Clear(); //배열 초기화
                            }
                            catch (Exception)
                            {
                                throw;
                            }
                        }
                        catch (Exception ex)
                        {
                            frm1.Log("[3-1차 2차 판매 표준 편차]:" + ex.Message);
                            frm1.Log("3-1차 2차  표준 편차쿼리:" + queryString1);

                        }
                    }

                    storeno_check = storeno; //매장코드 체크
                } //for()
                frm1.Log("[3-1차  2차  판매 표준 편차 완료]");
              
            }
            catch (Exception ex)
            {
                frm1.Log("[3-1차 2차  판매 표준 편차 pos_store_stock]:" + ex.Message);
                frm1.sendSms("[3-1차 2차  판매 표준 편차 pos_store_stock]:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();

        }

        private void fn_safe_standard_deviation_weekday_3_2(string safe_sale_sdate, string safe_sale_edate, string gubun,  Form1 frm1) //3-2단계 2단계 제외 수량을 제외한 요일별 표준편차
        {
            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("42");
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            SqlDataAdapter adapter = new SqlDataAdapter();
            SqlDataAdapter adapter1 = new SqlDataAdapter();

            MySqlDataAdapter adapter_my = new MySqlDataAdapter();
            MySqlDataAdapter adapter1_my = new MySqlDataAdapter();

            string table_name = "";
            if (gubun == "Y") //연간 표준편차
            {
                table_name = "preset_sale_ppm_daily_year";
            }
            else
            {
                table_name = "preset_sale_ppm_daily";
            }

           
            string queryString = " select storeno, ppmidx from preset_safe_stdevp_total where gubun = '" + gubun + "'  order by storeno, ppmidx  ";

            adapter_my.SelectCommand = new MySqlCommand(queryString, MYconn);
            MySqlCommand command = new MySqlCommand(queryString, MYconn);

            //adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            //SqlCommand command = new SqlCommand(queryString, myConnection);

            try
            {
                DataSet ds = new DataSet();
                adapter_my.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                double sale_count = 0d;
                string storeno_check = "";
                int except_2 = 0;

              
                double sun_safe = 0d;
                double mon_safe = 0d;
                double tue_safe = 0d;
                double wed_safe = 0d;
                double thu_safe = 0d;
                double fir_safe = 0d;
                double sat_safe = 0d;

                int sale_count_1 = 0;
                int sale_count_2 = 0;
                int sale_count_3 = 0;
                int sale_count_4 = 0;
                int sale_count_5 = 0;
                int sale_count_6 = 0;
                int sale_count_7 = 0;

                int sum_qty_1 = 0;
                int sum_qty_2 = 0;
                int sum_qty_3 = 0;
                int sum_qty_4 = 0;
                int sum_qty_5 = 0;
                int sum_qty_6 = 0;
                int sum_qty_7 = 0;

                int sale_minus_1 = 0;
                int sale_minus_2 = 0;
                int sale_minus_3 = 0;
                int sale_minus_4 = 0;
                int sale_minus_5 = 0;
                int sale_minus_6 = 0;
                int sale_minus_7 = 0;

                foreach (DataRow dr in rows)
                {
                    double sum_qty = 0d;

                    storeno = dr["storeno"].ToString();
                    ppmidx = dr["ppmidx"].ToString();
                    
                        if (storeno_check != storeno) //매장코드가 다르면
                        {

                        sale_count_1 = 0;
                        sale_count_2 = 0;
                        sale_count_3 = 0;
                        sale_count_4 = 0;
                        sale_count_5 = 0;
                        sale_count_6 = 0;
                        sale_count_7 = 0;


                        //요일별 매장영업 일수를 카운트
                        string queryString2 = " select  DATEPART(WEEKDAY,orderdate) as idx ,  count(idx) as sale_count from  pos_sale_A01  with (nolock) "
                                          //    + " where (orderdate >= '" + safe_sale_sdate + "' and orderdate <=  '" + safe_sale_edate + "') and right(storeno,4) = '" + storeno + "' and DATEPART(WEEKDAY,orderdate) = "+ a+" ";
                             //string queryString2 = " select  DATEPART(WEEKDAY,orderdate) as idx ,  count(orderdate) as sale_count from  "+table_name+"  with (nolock) "
                                                        + " where (orderdate >= '" + safe_sale_sdate + "' and orderdate <=  '" + safe_sale_edate + "') and right(storeno,4) = '" + storeno + "' group by DATEPART(WEEKDAY,orderdate)  ";

                            SqlCommand command2 = new SqlCommand(queryString2, myConnection);
                            SqlDataReader reader2 = command2.ExecuteReader();
                            while (reader2.Read())
                            {
                                int idx = Convert.ToInt32(reader2["idx"].ToString());
                                switch (idx)
                                {
                                    case 1:
                                        sale_count_1 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                    case 2:
                                        sale_count_2 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                    case 3:
                                        sale_count_3 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                    case 4:
                                        sale_count_4 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                    case 5:
                                        sale_count_5 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                    case 6:
                                        sale_count_6 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                    case 7:
                                        sale_count_7 = Convert.ToInt32(reader2["sale_count"].ToString());
                                        break;
                                }
                                        //sale_count = Convert.ToDouble(reader2["sale_count"].ToString());  //매장 영업 횟수
                            }
                            reader2.Close();
                        }

                     string queryString4 = " select except_2  from "
                                                                + "preset_safe_stdevp_total  "
                                                               + " where  storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  ";  //특이값 제외
                                                                                                                                            //string queryString3 = "select sum_jungga from preset_sale_ppm_jungga_total  where storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  ";


                    MySqlCommand command4 = new MySqlCommand(queryString4, MYconn);
                    MySqlDataReader reader4 = command4.ExecuteReader();

                    //SqlCommand command4 = new SqlCommand(queryString4, myConnection);
                    //SqlDataReader reader4 = command4.ExecuteReader();
                    while (reader4.Read())
                        {
                            except_2 = Convert.ToInt32(reader4["except_2"].ToString());
                        }
                        reader4.Close();


                    //정가판매량에서 제외값 확인

                    string queryString6 = "  select DAYOFWEEK(orderdate) as idx , count(ppmidx) as sale_minus from   " + table_name + "   "
                                                + " where storeno = '" + storeno + "' and ppmidx ='" + ppmidx + "' and (orderdate >= '" + safe_sale_sdate + "' and orderdate <=  '" + safe_sale_edate + "') and qty_jungga > " + except_2 + "  group by DAYOFWEEK(orderdate)  ";

                    MySqlCommand command6 = new MySqlCommand(queryString6, MYconn);
                    MySqlDataReader reader6 = command6.ExecuteReader();
                    //SqlCommand command6 = new SqlCommand(queryString6, myConnection);
                    //SqlDataReader reader6 = command6.ExecuteReader();
                    while (reader6.Read())
                    {
                        
                        int idx = Convert.ToInt32(reader6["idx"].ToString());
                        switch (idx)
                        {
                            case 1:
                                sale_minus_1 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                            case 2:
                                sale_minus_2 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                            case 3:
                                sale_minus_3 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                            case 4:
                                sale_minus_4 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                            case 5:
                                sale_minus_5 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                            case 6:
                                sale_minus_6 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                            case 7:
                                sale_minus_7 = Convert.ToInt32(reader6["sale_minus"].ToString());
                                break;
                        }
                    }
                    reader6.Close();

               
                    //요일별 매장 물품 총정가판매량 합계
                    string queryString3 = "select DAYOFWEEK(orderdate) as idx , ifnull(sum(qty_jungga),0)  as sum_jungga from " + table_name+"  where storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'  and (orderdate >= '" + safe_sale_sdate + "' and orderdate <= '" + safe_sale_edate + "') and qty_jungga <= " + except_2 + " group by  DAYOFWEEK(orderdate)  order by DAYOFWEEK(orderdate)     ";
                    MySqlCommand command3 = new MySqlCommand(queryString3, MYconn);
                    MySqlDataReader reader3 = command3.ExecuteReader();
                    //SqlCommand command3 = new SqlCommand(queryString3, myConnection);
                    //SqlDataReader reader3 = command3.ExecuteReader();
                    while (reader3.Read())
                        {
                        //sum_qty = Convert.ToDouble(reader3["sum_jungga"].ToString());
                        int idx1 = Convert.ToInt32(reader3["idx"].ToString());
                            switch (idx1)
                            {
                                case 1:
                                    sum_qty_1 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                                case 2:
                                    sum_qty_2 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                                case 3:
                                    sum_qty_3 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                                case 4:
                                    sum_qty_4 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                                case 5:
                                    sum_qty_5 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                                case 6:
                                    sum_qty_6 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                                case 7:
                                    sum_qty_7 = Convert.ToInt32(reader3["sum_jungga"].ToString());
                                    break;
                            }
                        }
                        reader3.Close();

                    for (int a = 1; a <= 7; a++) //요일별 판매내역 생성 (1:일 2:월, 3:화, 4:,수, 5:목, 6:금, 7:토)
                    {
                        switch (a)
                        {
                            case 1:
                                sale_count = sale_count_1 - sale_minus_1;
                                sum_qty = sum_qty_1;
                                break;
                            case 2:
                                sale_count = sale_count_2 - sale_minus_2;
                                sum_qty = sum_qty_2;
                                break;
                            case 3:
                                sale_count = sale_count_3 - sale_minus_3;
                                sum_qty = sum_qty_3;
                                break;
                            case 4:
                                sale_count = sale_count_4 - sale_minus_4;
                                sum_qty = sum_qty_4;
                                break;
                            case 5:
                                sale_count = sale_count_5 - sale_minus_5;
                                sum_qty = sum_qty_5;
                                break;
                            case 6:
                                sale_count = sale_count_6 - sale_minus_6;
                                sum_qty = sum_qty_6;
                                break;
                            case 7:
                                sale_count = sale_count_7 - sale_minus_7;
                                sum_qty = sum_qty_7;
                                break;
                        }

                        if (sum_qty != 0)
                        {
                            //배열에 입력될 날짜별 정가판매량계산 
                            string queryString1 = "  select  orderdate,   storeno, ppmidx, sum(qty_jungga) as qty_jungga   from  "
                                                                      + " "+table_name+"  "
                                                                       + "   where  storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "' and (orderdate >= '" + safe_sale_sdate + "' and orderdate <= '" + safe_sale_edate + "') and qty_jungga <= " + except_2 + "  and DAYOFWEEK(orderdate) = " + a + " "
                                                                      // + "   WHERE  (orderdate >='2018-01-01' and orderdate <= '2018-12-31') and storeno = '" + storeno.Substring(6, 4) + "'  AND   ppmidx = '" + ppmidx + "' "
                                                                      // + " where storeno ='8000006096' and  ppmidx ='10139'  "
                                                                      + " group by orderdate, storeno, ppmidx "
                                                                      + " order by orderdate ";
                            adapter1_my.SelectCommand = new MySqlCommand(queryString1, MYconn);
                            MySqlCommand command1 = new MySqlCommand(queryString1, MYconn);
                            //adapter1.SelectCommand = new SqlCommand(queryString1, myConnection);
                            //SqlCommand command1 = new SqlCommand(queryString1, myConnection);
                            try
                            {

                                DataSet ds1 = new DataSet();
                                adapter1_my.Fill(ds1);
                                DataTable table1 = ds1.Tables[0];
                                DataRowCollection rows1 = table1.Rows;

                                int qty_jungga = 0;
                                int check_count = 0;

                                List<int> list = new List<int>();

                                foreach (DataRow dr1 in rows1) //조회된 데이터만큼 루프
                                {
                                    qty_jungga = Convert.ToInt32(dr1["qty_jungga"].ToString());
                                    //storeno_1 = dr1["storeno"].ToString();

                                    list.Add(qty_jungga); //배열에 정가판매량 입력

                                    check_count += 1;
                                }

                                for (int i = check_count; i < sale_count; i++)  //영업일수만큼 판매량이 있어야되기 때문에 조회된 데이터외의 값은 0으로 입력(중요!)
                                {
                                    list.Add(0);
                                }

                                double average = sum_qty / sale_count;   //정가판매수량/영업일수
                                int valueCount = list.Count; //배열에 입력된 갯수

                                if (valueCount == 0)
                                {
                                    frm1.Log("[배열에 입력된 값없음]:" + valueCount);
                                }


                                double standardDeviation = 0d;
                                double variance = 0d;

                                try
                                {
                                    for (int i = 0; i < valueCount; i++)
                                    {
                                        variance += Math.Pow(list[i] - average, 2); //제곱근 함수 
                                    }
                                    standardDeviation = Math.Sqrt(SafeDivide(variance, valueCount)); //루트 함수

                                    switch (a)
                                    {
                                        case 1:
                                            sun_safe = Math.Round(standardDeviation, 2);
                                            break;
                                        case 2:
                                            mon_safe = Math.Round(standardDeviation, 2);
                                            break;
                                        case 3:
                                            tue_safe = Math.Round(standardDeviation, 2);
                                            break;
                                        case 4:
                                            wed_safe = Math.Round(standardDeviation, 2);
                                            break;
                                        case 5:
                                            thu_safe = Math.Round(standardDeviation, 2);
                                            break;
                                        case 6:
                                            fir_safe = Math.Round(standardDeviation, 2);
                                            break;
                                        case 7:
                                            sat_safe = Math.Round(standardDeviation, 2);
                                            break;
                                    }

                                    list.Clear(); //배열 초기화
                                 
                                }
                                catch (Exception)
                                {
                                    frm1.Log("3-2차 요일별 입력 오류:" + storeno + ": "+ppmidx);
                                    throw;
                                    
                                }
                            }
                            catch (Exception ex)
                            {
                                frm1.Log("[3-2차 요일별 판매 표준 편차]:" + ex.Message);
                                frm1.sendSms("[3-2차 요일별 판매 표준 편차]:" + ex.Message);
                                frm1.Log("3-2차 요일별 표준 편차쿼리:" + queryString1);
                            }
                        }
                    }

                    try
                    {
                        string queryString5 = "insert into preset_safe_weekday (storeno,ppmidx,sun_safe,mon_safe,tue_safe,wed_safe,thu_safe,fir_safe,sat_safe,gubun)  values "
                                                        + "('" + storeno + "' ,'" + ppmidx + "' ," + sun_safe + "," + mon_safe + "," + tue_safe + "," + wed_safe + "," + thu_safe + "," + fir_safe + "," + sat_safe + ",'" + gubun + "')";

                       // frm1.Log((String.Format("[in_other_log] 쿼리[{0}] : ", queryString5)));
                        command.CommandText = queryString5;
                        command.ExecuteNonQuery();

                        sale_minus_1 = 0;
                        sale_minus_2 = 0;
                        sale_minus_3 = 0;
                        sale_minus_4 = 0;
                        sale_minus_5 = 0;
                        sale_minus_6 = 0;
                        sale_minus_7 = 0;

                        sale_count = 0;
                        sum_qty = 0;

                        sun_safe = 0;
                        mon_safe = 0;
                        tue_safe = 0;
                        wed_safe = 0;
                        thu_safe = 0;
                        fir_safe = 0;
                        sat_safe = 0;
                     
                        sum_qty_1 = 0;
                        sum_qty_2 = 0;
                        sum_qty_3 = 0;
                        sum_qty_4 = 0;
                        sum_qty_5 = 0;
                        sum_qty_6 = 0;
                        sum_qty_7 = 0;
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    storeno_check = storeno; //매장코드 체크
                } //for()

                frm1.Log("[3-2차 요일별 판매 표준 편차 완료]");
              
            }
            catch (Exception ex)
            {
                frm1.Log("[3-2차 요일별 판매 표준 편차 pos_product_date]:" + ex.Message);
                frm1.sendSms("[3-2차 요일별 판매 표준 편차 pos_product_date]:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();

        }

        private void fn_safe_standard_deviation_ppm_year(string safe_sale_sdate, string safe_sale_edate, Form1 frm1) //작년도를 정가판매량 테이블로 백업
        {
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            string sql = null;
            string sql_1 = null;
            SqlCommand command = new SqlCommand(sql, myConnection);
            SqlDataAdapter adapter = new SqlDataAdapter();

            try
            {
                sql = " insert into preset_sale_ppm_daily_year (orderdate, storeno, ppmidx, qty_jungga, qty_total, qty_discount) "
                        + " select  orderdate, storeno, ppmidx, qty_jungga, qty_total, qty_discount "
                        + " from preset_sale_ppm_daily  where (orderdate >= '" + safe_sale_sdate + "'and  orderdate <= '" + safe_sale_edate + "' ) ";
                command.CommandText = sql;
                command.CommandTimeout = 0;
                command.ExecuteNonQuery();
                try
                {
                    sql_1 = " delete from preset_sale_ppm_daily where(orderdate >= '" + safe_sale_sdate + "'and orderdate <= '" + safe_sale_edate + "' )";
                    command.CommandText = sql_1;
                    command.CommandTimeout = 0;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    frm1.sendSms("[작년도 정가판매량 백업 삭제]오류:" + ex.Message);
                    frm1.Log("[작년도 정가판매량 백업 삭제]오류:" + ex.Message);
                }

            }
            catch (Exception ex)
            {
                frm1.sendSms("[작년도 정가판매량 백업]오류:" + ex.Message);
                frm1.Log("[작년도 정가판매량 백업]오류:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();

        }

        private void fn_get_safe_standard_deviation_log(Form1 frm1)  //전월 ABC누적 백업
        {

            DateTime today = DateTime.Now.Date; //현재 날짜 확인
            var ex_today = today.AddMonths(0); // 월 구함
            var ex_sdate = ex_today.AddDays(1 - today.Day); //현재 날짜 기준의 월의 날짜 1일 구함

            string safe_sale_sdate = string.Format(ex_sdate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            string sql = null;
            string sql_1 = null;
            SqlCommand command = new SqlCommand(sql, myConnection);
            SqlDataAdapter adapter = new SqlDataAdapter();

            try
            {
                sql = " insert into preset_scm_stock_share_log  "
                        + " select  storeno, ppmidx, total_jungga, share, sum_share, grade,  getdate(),'" + safe_sale_sdate + "'   "
                        + " from preset_scm_stock_share  ";
                command.CommandText = sql;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();
                try
                {
                    sql_1 = " delete preset_scm_stock_share ";
                    command.CommandText = sql_1;
                    command.CommandTimeout = 120;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    frm1.sendSms("[전월 ABC누적 삭제]오류:" + ex.Message);
                    frm1.Log("[전월 ABC누적 삭제]오류:" + ex.Message);
                }

            }
            catch (Exception ex)
            {
                frm1.sendSms("[전월 ABC누적 백업]오류:" + ex.Message);
                frm1.Log("[전월 ABC누적 백업]오류:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();
        }
        private void fn_get_safe_standard_deviation_total(string sdate, string edate, string gubun, Form1 frm1)//전월 ABC누적 생성
        {
            string table_name = "";
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlDataAdapter adapter = new SqlDataAdapter();

            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("42");
            MySqlDataAdapter adapter_my = new MySqlDataAdapter();

            sdate = "2018-12-01";
            edate = "2018-12-31";

            string queryString1 = "";
            string stock_date = string.Format(DateTime.Today.AddDays(0).ToString("yyyy-MM-dd"));

            if (gubun == "Y") //연간 정가 판매율일 경우
            {
                 table_name = "preset_sale_ppm_daily_year";
            }
            else
            {
                table_name = "preset_sale_ppm_daily";
            }

            string queryString = "SELECT  right(a.storeno,4) as storeno  "
               + "  FROM pos_store_detail as a "
               + "  LEFT JOIN pos_store b ON b.storeno = a.storeno "
              + " WHERE b.vc_businessStatus = '1' AND a.store_mcode <> '' AND b.storeType <> 'A' "
               + " AND a.sdate <= '" + stock_date + "'  AND (a.edate = '' OR a.edate >= '" + stock_date + "') "
               + " and a.storeno not in ('8000006996','8000009011','8000009017','8000006158') ";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            SqlCommand command = new SqlCommand(queryString, myConnection);

            MySqlCommand command_my = new MySqlCommand(queryString, MYconn);

            try
            {
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;
                string storeno = "";

                foreach (DataRow dr in rows)
                {
                    storeno = dr["storeno"].ToString();

                    queryString1 = " insert into preset_scm_stock_share (storeno, ppmidx, total_jungga, share, sum_share,grade)"
                                               + " select  a.storeno,4 as storeno, a.ppmidx,    "
                                               + "   sum(a.qty_jungga)as qty_jungga, "
                                               + "   round(sum(a.qty_jungga)/convert(float,c.jungga_total)*100,2) as avg_jungga,  "
                                               + "   round(sum(sum(a.qty_jungga)/convert(float,c.jungga_total)*100) over(order by sum(a.qty_jungga) desc),1) as running   "
                                               + "      , case    "
                                               + "    when round(sum(sum(a.qty_jungga)/convert(float,c.jungga_total)*100) over(order by sum(a.qty_jungga) desc),1) < 80 then 'A'  "
                                               + "    when round(sum(sum(a.qty_jungga)/convert(float,c.jungga_total)*100) over(order by sum(a.qty_jungga) desc),1) > 80 and round(sum(sum(a.qty_jungga)/convert(float,c.jungga_total)*100) over(order by sum(a.qty_jungga) desc),1) < 90 then 'B'  "
                                               + "    when round(sum(sum(a.qty_jungga)/convert(float,c.jungga_total)*100) over(order by sum(a.qty_jungga) desc),1) >= 90 then 'C'  "
                                               + "    else ''  "
                                               + "   end "
                                               + "	  from "
                                               + "	 ( "
                                               + "		    select storeno ,  sum(qty_jungga) as jungga_total "
                                               + "		    from "+ table_name+"   "
                                               + "		    where  storeno ='" + storeno + "' and (orderdate >='" + sdate + "' and orderdate <='" + edate + "')  "
                                               + "		    group by storeno "
                                               + "	  ) as c "
                                               + "	     left join  "
                                               + "	 (  "
                                               + "          select storeno, ppmidx, sum(qty_jungga)as qty_jungga "
                                               + "          from  " + table_name + "  "
                                              + "		      where  storeno ='" + storeno + "' and (orderdate >='" + sdate + "' and orderdate <='" + edate + "')  "
                                               + "          group by storeno, ppmidx "
                                               + "   ) as a on a.storeno = c.storeno "
                                               + " group by a.storeno, a.ppmidx,c.jungga_total ";


                    command_my.CommandText = queryString1;
                    command_my.ExecuteNonQuery();

                }

                frm1.sendSms("매월1일 ABC 누적 데이터 생성 완료");
                frm1.Log("매월1일 ABC 누적 데이터 생성 완료");

            }
            catch (Exception ex)
            {
                frm1.Log("[매월1일 ABC 누적 데이터 생성 에러 ]:" + ex.Message);
                frm1.Log("매월1일 ABC 누적 데이터 생성 쿼리:" + queryString1);
                frm1.sendSms("매월1일 ABC 누적 데이터 생성 오류");
            }

            myConnection.Dispose();
            myConnection.Close();

        }

        private void fn_get_safe_calculate(string gubun, Form1 frm1)  //안전재고량 산출
        {

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            SqlDataAdapter adapter = new SqlDataAdapter();

            //string queryString = " select storeno, ppmidx,  sun_safe, mon_safe, tue_safe, wed_safe, thu_safe, fir_safe, sat_safe  from preset_safe_weekday with (nolock) where gubun = '" + gubun + "' order by storeno, ppmidx  ";
            string queryString = " select   a.storeno, a.ppmidx,  a.sun_safe, a.mon_safe, a.tue_safe, a.wed_safe, a.thu_safe, a.fir_safe, a.sat_safe, b.grade    "
                                                    + " from  preset_safe_weekday as a "
                                                    + " left join preset_scm_stock_share as b on a.storeno = b.storeno and a.ppmidx = b.ppmidx "
                                                    + " where  a.gubun = '" + gubun + "' and  b.grade is not null "
                                                    //+ " and a.storeno ='6012' and a.ppmidx ='25695' ";
                                                    //  + "    and a.storeno in (select storeno from preset_product_store) ";
                                                    + " and a.storeno in( '6012','6085','6223') and a.ppmidx in (select idx from  pos_product_master where pcmidx in ('59' ,'32' ,'34','51')) ";
            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            SqlCommand command = new SqlCommand(queryString, myConnection);

            try
            {
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                string grade = "";
                double sun_safe = 0d;
                double mon_safe = 0d;
                double tue_safe = 0d;
                double wed_safe = 0d;
                double thu_safe = 0d;
                double fir_safe = 0d;
                double sat_safe = 0d;

                double safe_sun = 0d;
                double safe_mon = 0d;
                double safe_tue = 0d;
                double safe_wed = 0d;
                double safe_thu = 0d;
                double safe_fir = 0d;
                double safe_sat = 0d;

                foreach (DataRow dr in rows)
                {

                    storeno = dr["storeno"].ToString();
                    ppmidx = dr["ppmidx"].ToString();
                    grade = dr["grade"].ToString();

                    sun_safe = Convert.ToDouble(dr["sun_safe"].ToString());
                    mon_safe = Convert.ToDouble(dr["mon_safe"].ToString());
                    tue_safe = Convert.ToDouble(dr["tue_safe"].ToString());
                    wed_safe = Convert.ToDouble(dr["wed_safe"].ToString());
                    thu_safe = Convert.ToDouble(dr["thu_safe"].ToString());
                    fir_safe = Convert.ToDouble(dr["fir_safe"].ToString());
                    sat_safe = Convert.ToDouble(dr["sat_safe"].ToString());

                    double grade_price = 0d;
             
                    if (storeno == "6012")
                    {
                        switch (grade)
                        {
                            case "A":
                                grade_price = 2.58;
                                break;
                            case "B":
                                grade_price = 1.65;
                                break;
                            case "C":
                                grade_price = 1.44;
                                break;
                        }
                    }
                    else { 
                        switch (grade)
                        {
                            case "A":
                                grade_price = 1.96;
                                break;
                            case "B":
                                grade_price = 1.65;
                                break;
                            case "C":
                                grade_price = 1.44;
                                break;
                        }
                    }

                    //소수점 둘째자리 반올림 후 최소진열 수량과 비교
                    // 크다면 첫째자리에서 반올림
                    safe_sun = Math.Round(grade_price * (Math.Sqrt(2) * sun_safe), 1, MidpointRounding.AwayFromZero);
                     if(safe_sun < 3)  {  safe_sun = 3;  } else  {  safe_sun = Math.Round(safe_sun, 0, MidpointRounding.AwayFromZero); }

                     safe_mon = Math.Round(grade_price * (Math.Sqrt(2) * mon_safe), 1, MidpointRounding.AwayFromZero);
                    if (safe_mon < 3) { safe_mon = 3;  } else {  safe_mon = Math.Round(safe_mon, 0, MidpointRounding.AwayFromZero); }

                    safe_tue = Math.Round(grade_price * (Math.Sqrt(2) * tue_safe), 1, MidpointRounding.AwayFromZero);
                    if (safe_tue < 3) { safe_tue = 3; }  else  { safe_tue = Math.Round(safe_tue, 0, MidpointRounding.AwayFromZero);  }

                    safe_wed = Math.Round(grade_price * (Math.Sqrt(2) * wed_safe), 1, MidpointRounding.AwayFromZero);
                    if (safe_wed < 3) { safe_wed = 3; }  else {  safe_wed = Math.Round(safe_wed, 0, MidpointRounding.AwayFromZero);  }

                    safe_thu = Math.Round(grade_price * (Math.Sqrt(2) * thu_safe), 1, MidpointRounding.AwayFromZero);
                    if (safe_thu < 3) { safe_thu = 3; }  else  {  safe_thu = Math.Round(safe_thu, 0, MidpointRounding.AwayFromZero);  }

                    safe_fir = Math.Round(grade_price * (Math.Sqrt(2) * fir_safe), 1, MidpointRounding.AwayFromZero);
                    if (safe_fir < 3) { safe_fir = 3; } else   { safe_fir = Math.Round(safe_fir, 0, MidpointRounding.AwayFromZero);  }

                    safe_sat = Math.Round(grade_price * (Math.Sqrt(2) * sat_safe), 1, MidpointRounding.AwayFromZero);
                    if (safe_sat < 3) { safe_sat = 3; }  else { safe_sat = Math.Round(safe_sat,0, MidpointRounding.AwayFromZero);   }


                    string queryString1 = " IF EXISTS (SELECT * FROM preset_safe_stock WHERE storeno = '" + storeno + "' AND ppmidx ='" + ppmidx + "' ) "
                                                            + " BEGIN "
                                                            + " UPDATE preset_safe_stock  "
                                                            + " SET amt_safe_mon = " + safe_mon + ", amt_safe_tue = " + safe_tue + ", amt_safe_wed = " + safe_wed + ", amt_safe_thu = " + safe_thu + ", "
                                                            + " amt_safe_fri = " + safe_fir + ", amt_safe_sat = " + safe_sat + ", amt_safe_sun = " + safe_sun +", finalUpdate = getdate(), finalAdmin = 'DEMON_2' "
                                                            + " WHERE storeno = '" + storeno + "' AND ppmidx ='" + ppmidx + "' "
                                                            + " END "
                                                            + " ELSE "
                                                            + " BEGIN "
                                                            + " INSERT INTO preset_safe_stock (ppmidx, storeno, amt_safe_mon, amt_safe_tue, amt_safe_wed, amt_safe_thu, amt_safe_fri, amt_safe_sat, amt_safe_sun, regdate, finalAdmin) "
                                                            + " VALUES ('" + ppmidx + "','" + storeno + "'," + safe_mon + "," + safe_tue + "," + safe_wed + "," + safe_thu + "," + safe_fir + "," + safe_sat + "," + safe_sun + ",getdate(),'DEMON_2')"
                                                            + " END ";
                    //Log((String.Format("[in_other_log] 쿼리[{0}] : ", queryString1)));
                    command.CommandText = queryString1;
                    command.ExecuteNonQuery();


                    // 테스트 시만 주석처리!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    string queryString5 = " INSERT INTO preset_safe_stock_back (ppmidx, storeno, amt_safe_mon, amt_safe_tue, amt_safe_wed, amt_safe_thu, amt_safe_fri, amt_safe_sat, amt_safe_sun, regdate, finalUpdate, finalAdmin) "
                                                            + " SELECT ppmidx, storeno, amt_safe_mon, amt_safe_tue, amt_safe_wed, amt_safe_thu, amt_safe_fri, amt_safe_sat, amt_safe_sun, regdate, finalUpdate, finalAdmin "
                                                            + " FROM preset_safe_stock_ex   "
                                                            + " WHERE storeno = '" + storeno + "' AND ppmidx ='" + ppmidx + "' ";

                    ////Log((String.Format("[in_other_log] 쿼리[{0}] : ", queryString1)));
                    command.CommandText = queryString5;
                    command.ExecuteNonQuery();


                }
            }
            catch (Exception ex)
            {
                frm1.sendSms("[안전재고량생성]오류:" + ex.Message);
                frm1.Log("[안전재고량생성]오류:" + ex.Message);
            }

            myConnection.Dispose();
            myConnection.Close();
        }


        private void fn_safe_standard_deviation_delete(string gubun, Form1 frm1) //작년도를 정가판매량 테이블로 백업
        {
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            string sql = null;
            string sql_1 = null;
            SqlCommand command = new SqlCommand(sql, myConnection);
            SqlDataAdapter adapter = new SqlDataAdapter();

            try
            {
                sql_1 = " delete preset_safe_stdevp_total where gubun = '" + gubun + "'";
                command.CommandText = sql_1;
                command.CommandTimeout = 0;
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                frm1.sendSms("[표준편차 삭제]오류:" + ex.Message);
                frm1.Log("[표준편차 삭제]오류:" + ex.Message);
            }
 
            myConnection.Dispose();
            myConnection.Close();

        }

    }
}
