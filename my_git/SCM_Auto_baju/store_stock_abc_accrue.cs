using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Windows.Forms;

namespace SCM_Auto_baju
{
    class store_stock_abc_accrue
    {
        SqlConnection myConnection = new SqlConnection();

        public void fn_store_stock_abc_accrue(string sdate, string edate, Form1 frm1)
        {
            frm1.sendSms("매월1일 ABC 누적 데이터 생성 시작");
            fn_get_safe_standard_deviation_log(frm1); //매월1일 ABC 누적 데이터  생성
            frm1.sendSms("전월 ABC누적 데이터 백업 완료");
            fn_get_safe_standard_deviation_total(sdate, edate, frm1); //매월1일 ABC 누적 데이터  생성
            frm1.sendSms("매월1일 ABC 누적 데이터 생성 종료");
        }

        private void fn_get_safe_standard_deviation_log(Form1 frm1)
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
                        + " select '"+ safe_sale_sdate+"',  storeno, ppmidx, total_jungga, share, sum_share, grade   "
                        + " from preset_scm_stock_share  ";
                command.CommandText = sql;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();
                try
                {
                    sql_1 = " delete from preset_scm_stock_share ";
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
        private void fn_get_safe_standard_deviation_total(string sdate, string edate, Form1 frm1)
        {

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlDataAdapter adapter = new SqlDataAdapter();

            string queryString1 = "";
            string stock_date = string.Format(DateTime.Today.AddDays(0).ToString("yyyy-MM-dd"));

            string queryString = "SELECT a.storeno  "
               + "  FROM pos_store_detail as a "
               + "  LEFT JOIN pos_store b ON b.storeno = a.storeno "
              + " WHERE b.vc_businessStatus = '1' AND a.store_mcode <> '' AND b.storeType <> 'A' "
               + " AND a.sdate <= '" + stock_date + "'  AND (a.edate = '' OR a.edate >= '"+ stock_date + "') "
               + " and a.storeno not in ('8000006996','8000009011','8000009017','8000006158') ";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            SqlCommand command = new SqlCommand(queryString, myConnection);

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

                     //queryString1 = " insert into preset_scm_stock_share (storeno, ppmidx, total_jungga, share, cum_share,grade)"
                     //                            + " select b.*, "
                     //                            + "   (case when running < 80 then 'A' when running > 80 and running < 90 then 'B' when running >= 90 then 'C' else '' end) as grade "
                     //                            + "      from "
                     //                            + "   (   "
                     //                            + "    select   a.storeno, a.ppmidx,   "
                     //                            + "    sum(a.qty_jungga)as qty_jungga,"
                     //                            + "    round(sum(a.qty_jungga)/convert(float,c.jungga_total)*100,2) as avg_jungga, "
                     //                            + "    round(sum(sum(a.qty_jungga)/convert(float,c.jungga_total)*100) over(order by sum(a.qty_jungga) desc),1) as running from "
                     //                            + "    	( "
                     //                            + "         select  right(storeno,4) as storeno ,  sum(qty_jungga) as jungga_total from preset_sale_ppm_daily  "
                     //                           + "	        where  storeno ='" + storeno + "' and (orderdate >='" + sdate + "' and orderdate <='" + edate + "')"
                     //                            + "	        group by storeno"
                     //                            + "		) as c"
                     //                            + "		left join"
                     //                            + "		( "
                     //                            + "		    select  right(storeno,4) as storeno, ppmidx,sum(qty_jungga)as qty_jungga   from "
                     //                            + "	        preset_sale_ppm_daily "
                     //                            + "	        where  storeno ='" + storeno + "' and (orderdate >='" + sdate + "' and orderdate <='" + edate + "')"
                     //                            + "	        group by storeno, ppmidx "
                     //                            + "       ) as a on a.storeno = c.storeno"
                     //                            + "    group by a.storeno, a.ppmidx,c.jungga_total"
                     //                            + " ) as b "
                     //                            + " group by b.storeno, b.ppmidx,b.qty_jungga,b.avg_jungga,b.running "
                     //                            + "order by storeno, sum(qty_jungga) desc ";
                    queryString1 = " insert into preset_scm_stock_share (storeno, ppmidx, total_jungga, share, cum_share,grade)"
                                               + " select a.storeno, a.ppmidx,    "
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
                                               + "		    from preset_sale_ppm_daily   "
                                               + "		    where  storeno ='" + storeno + "' and (orderdate >='" + sdate + "' and orderdate <='" + edate + "')  "
                                               + "		    group by storeno "
                                               + "	  ) as c "
                                               + "	     left join  "
                                               + "	 (  "
                                               + "          select storeno, ppmidx, sum(qty_jungga)as qty_jungga "
                                               + "          from preset_sale_ppm_daily  "
                                              + "		      where  storeno ='" + storeno + "' and (orderdate >='" + sdate + "' and orderdate <='" + edate + "')  "
                                               + "          group by storeno, ppmidx "
                                               + "   ) as a on a.storeno = c.storeno "
                                               + " group by a.storeno, a.ppmidx,c.jungga_total ";

   
                    command.CommandText = queryString1;
                    command.ExecuteNonQuery();

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
    }   
}
