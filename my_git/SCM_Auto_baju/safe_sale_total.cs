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
    class safe_sale_total
    {
        SqlConnection myConnection = new SqlConnection();

        public void fn_safe_sale(string safe_sale_sdate, string safe_sale_edate, string safe_junnga, string stock_YYYYMM,  string type, int totlaDay, Form1 frm1)
        {
            //frm1.sendSms("매월 1일  안전재고, 정가판매 " + safe_sale_sdate + "~" + safe_sale_edate + "  생성 시작");
            fn_get_safe_sale_total(safe_sale_sdate, safe_sale_edate, safe_junnga, stock_YYYYMM, type, totlaDay, frm1); //안전재고량, 정가판매 통합 자료 생성
        }

        private void fn_get_safe_sale_total(string safe_sale_sdate, string safe_sale_edate, string safe_junnga, string stock_YYYYMM, string type, int totlaDay, Form1 frm1)
        {
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlCommand command = new SqlCommand();

            String sql = null;
            String safe_sale_table = null;

            try
            {
                //이전 생성 자료 로그 저장
                sql = "IF EXISTS (SELECT top 1 * FROM preset_scm_safe_sale_date ) "
                    + " BEGIN "
                    + " INSERT INTO preset_scm_safe_sale_date_back ( storeno, ppmidx, qty_jungga, qty_safe, regdate )  SELECT storeno, ppmidx, qty_jungga, qty_safe ,getdate() FROM preset_scm_safe_sale_date  "
                    + " DELETE preset_scm_safe_sale_date  "
                    + " END";
            
            command.Connection = myConnection;
            command.CommandText = sql;
            command.CommandTimeout = 120;
            command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                frm1.Log("[매월 안전재고 정가판매 백업]:" + ex.Message);
                frm1.Log("매월 안전재고 정가판매 백업쿼리:" + sql);
                frm1.sendSms("매월 1일  안전재고, 정가판매 백업 오류 ");
            }

       
            safe_sale_table = "pos_store_stock_" + stock_YYYYMM + " ";
 
                try
            {
                /*
                  * 한달간 재고량합/월별일수로 기준데이터 생성
                  * 일주일간 안전재고 수량이 0<=3 적으며
                  * 한달간 정가판매수량이 0<=7작은 내역 중 
                  * 두개의 조건이 포함(AND)되는 것들만 저장
                 */
                if (type == "A")
                {
                    sql = " INSERT INTO preset_scm_safe_sale_date (storeno, ppmidx, qty_jungga, qty_safe)  "
                       + " select right(a.storeno,4) as storeno, a.ppmidx,isnull(c.jungga,0) as jungga,  isnull(b.safe,0) as safe "
                       + "  from  "
                        + "( "
                        + " select storeno, ppmidx "
                        + " from pos_store_stock with(nolock)  where storeno <> '' and ppmidx <> 0  "
                        + " group by storeno, ppmidx "
                        + " having round(sum(amt_result)/convert(float,"+totlaDay+"),1) > 0 "
                        + " UNION ALL "
                        + " select storeno, ppmidx "
                        + " from  " + safe_sale_table + " with(nolock) where  storeno <> '' and ppmidx <> 0  "
                        + " group by storeno, ppmidx "
                        + " having round(sum(amt_result)/convert(float,"+ totlaDay+"),1) > 0 "
                        + ") as a "
                        + "left join "
                        + "( "
                       + "  select storeno, ppmidx, (sum(amt_safe_fri+amt_safe_mon+amt_safe_sat+amt_safe_sun+amt_safe_thu+amt_safe_tue+amt_safe_wed+amt_safe_wed) /7 ) as safe "
                       + "  from preset_safe_stock with(nolock)  "
                       + "  group by storeno, ppmidx "
                  //     + "  having sum(amt_safe_fri+amt_safe_mon+amt_safe_sat+amt_safe_sun+amt_safe_thu+amt_safe_tue+amt_safe_wed+amt_safe_wed) /7 <= 3 "
                       + " ) as b on right(a.storeno,4) = b.storeno and convert(varchar,a.ppmidx) = b.ppmidx  "
                       + "  left join ( "
                       + " select storeno, ppmidx, sum(qty_jungga)  as jungga "
                       + " from preset_sale_ppm_daily with(nolock)  "
                       + " where orderdate >='" + safe_sale_sdate + "' and orderdate <='" + safe_sale_edate + "'   "
                       + " group by storeno, ppmidx "
                     //  + "  having sum(qty_jungga) <= " + safe_junnga + " "
                       + " ) as c on right(a.storeno,4) = c.storeno and convert(varchar,a.ppmidx) = c.ppmidx  "
                       + "  where a.storeno is  not null and b.safe <> 0  "
                       + " order by storeno, ppmidx  ";
                }
                else
                {
                    sql = " INSERT INTO preset_scm_safe_sale_date (storeno, ppmidx, qty_jungga, qty_safe)  "
                      + " select right(a.storeno,4) as storeno, a.ppmidx,isnull(c.jungga,0) as jungga,  isnull(b.safe,0) as safe "
                      + "  from  "
                       + "( "
                       + " select storeno, ppmidx "
                       + " from  " + safe_sale_table + " with(nolock) where  storeno <> '' and ppmidx <> 0  "
                       + " group by storeno, ppmidx "
                       + " having round(sum(amt_result)/convert(float,30),1) > 0 "
                       + ") as a "
                       + "left join "
                       + "( "
                      + "  select storeno, ppmidx, (sum(amt_safe_fri+amt_safe_mon+amt_safe_sat+amt_safe_sun+amt_safe_thu+amt_safe_tue+amt_safe_wed+amt_safe_wed) /7 ) as safe "
                      + "  from preset_safe_stock with(nolock)  "
                      + "  group by storeno, ppmidx "
                    //  + "  having sum(amt_safe_fri+amt_safe_mon+amt_safe_sat+amt_safe_sun+amt_safe_thu+amt_safe_tue+amt_safe_wed+amt_safe_wed) /7 <= 3 "
                      + " ) as b on right(a.storeno,4) = b.storeno and convert(varchar,a.ppmidx) = b.ppmidx  "
                      + "  left join ( "
                      + " select storeno, ppmidx, sum(qty_jungga)  as jungga "
                      + " from preset_sale_ppm_daily with(nolock)  "
                      + " where orderdate >='" + safe_sale_sdate + "' and orderdate <='" + safe_sale_edate + "'   "
                      + " group by storeno, ppmidx "
                     // + "  having sum(qty_jungga) <= " + safe_junnga + " "
                      + " ) as c on  right(a.storeno,4) = c.storeno and convert(varchar,a.ppmidx) = c.ppmidx  "
                      + "  where a.storeno is  not null and b.safe <> 0  "
                      + " order by storeno, ppmidx  ";

                }
                command.Connection = myConnection;
                command.CommandText = sql;
                command.CommandTimeout = 120;
                command.ExecuteNonQuery();

                frm1.sendSms("생성기준 매월 1일  안전재고,정가판매 제외 " + safe_sale_sdate + "~" + safe_sale_edate + "  생성 완료");
            }
            catch (Exception ex)
            {
                frm1.Log("[매월 안전재고 정가판매 생성]:" + ex.Message);
                frm1.Log("매월 안전재고 정가판매 생성쿼리:" + sql);
                frm1.sendSms("매월 1일  안전재고, 정가판매 오류 ");
            }

        }
    }
}
