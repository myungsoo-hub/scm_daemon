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
    class store_product_unpopular
    {
        SqlConnection myConnection = new SqlConnection();

        public void fn_unpopular_list(string orderMonth, Form1 frm1)
        {
            //frm1.sendSms("매월 1일  안전재고, 정가판매 " + safe_sale_sdate + "~" + safe_sale_edate + "  생성 시작");
            fn_unpopular(orderMonth, frm1); //안전재고량, 정가판매 통합 자료 생성
        }

        private void fn_unpopular(string orderMonth, Form1 frm1)
        {
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            try
            {
                if (orderMonth == "")
                {
                    frm1.Log(Convert.ToDateTime(DateTime.Today.ToString()) + "[비인기품목생성에러]생성년/월 확인");
                }
                else
                {
                    //orderMonth = "201710";
                    //전월 1일
                    string sdate = orderMonth.Substring(0, 4) + "-" + orderMonth.Substring(4, 2) + "-01";
                    sdate = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((sdate)).AddMonths(-1));

                    //전월 마지막 일자
                    string edate = orderMonth.Substring(0, 4) + "-" + orderMonth.Substring(4, 2) + "-01";
                    edate = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((edate)).AddDays(-1));

                    //전월 마지막 일자 - 2개월
                    string edate_2m = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((edate)).AddMonths(-2));

                    //전월 마지막 일자 - 3개월
                    string edate_3m = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime((edate)).AddMonths(-3));


                    //비인기품목 리스트
                    string queryString = "INSERT INTO store_product_unpopular (ordermonth, storeno, ppmidx, amt_sale, bad_stock_gb, rdate_sale, regdate) "
                                        + " SELECT '" + orderMonth + "' as orderMonth, right(a.storeNo,4) as storeno, a.ppmidx, "
                                        + " ISNULL(b.qty_total,0),"
                                        + " CASE WHEN c.rdate_sale <= '" + edate_3m + "' OR c.rdate_sale is null THEN 'Y' ELSE 'N' END as bad_stock_gb, "
                                        + " ISNULL(c.rdate_sale, ''), getdate()"
                                        + " FROM (SELECT ppmidx, storeno FROM pos_store_stock_reality with (nolock)"
                                        + "         WHERE orderdate >= '" + sdate + "' AND orderDate <= '" + edate + "'"
                                        + "         AND ppmidx in (SELECT idx FROM pos_product_master WHERE regdate < '" + edate_2m + "' and pcmidx not in (111, 24, 110, 26, 113, 112, 25)) "
                                        + "         GROUP BY storeno, ppmidx"
                                        + " ) as a"
                                        + " LEFT OUTER JOIN (SELECT storeno, ppmidx, SUM(qty_total-qty_g08-qty_g13) as qty_total "
                                        + "             FROM pos_sale_A01_ppm_daily with (nolock)"
                                        + "             WHERE orderdate >= '" + sdate + "' AND orderdate <= '" + edate + "'"
                                        + "             AND ppmidx in (SELECT idx FROM pos_product_master WHERE regdate < '" + edate_2m + "' and pcmidx not in (111, 24, 110, 26, 113, 112, 25)) "
                                        + "             GROUP BY storeno, ppmidx"
                                        + " ) as b on a.storeno = b.storeno and a.ppmidx = b.ppmidx"
                                        + " LEFT OUTER JOIN pos_store_stock as c with (nolock) ON a.storeno = c.storeno and a.ppmidx = c.ppmidx"
                                        + " WHERE ISNULL(b.qty_total,0) <= 30"
                                        + " ORDER BY a.storeno, a.ppmidx";

                    SqlCommand command = new SqlCommand(queryString, myConnection);
                    command.CommandTimeout = 120;
                    command.CommandText = queryString;
                    command.ExecuteNonQuery();
                    frm1.Log("[비인기품목생성에러]\n" + queryString);
                }
                myConnection.Dispose();
                myConnection.Close();
                frm1.sendSms("매월 24일  비인기품목생성 완료");
            }
            catch (Exception e)
            {
                frm1.sendSms("[비인기품목생성에러]\n" + e.Message);
                frm1.Log(Convert.ToDateTime(DateTime.Today.ToString()) + "[비인기품목생성에러]\n" + e.Message);
            }
        }
    }
}
