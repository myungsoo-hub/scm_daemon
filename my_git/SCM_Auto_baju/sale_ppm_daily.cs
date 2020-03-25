using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Windows.Forms;

/*
 * 2019-07-10
 * 금액 11시에 당일  pos_sale_orderNo_detail_YYYYMM 테이블 데이터를 바탕으로 정가판매량 저장
 * 당일 매장 마감이 전체 이루어지지 않고 11시 이후 판매 되었을 경우 재고오차는 발생
 * 낱개 박스로 연결된 물품일경우 박스로 판매되었을경우 낱개 정가판매량에 반영
 */

namespace SCM_Auto_baju
{
    class sale_ppm_daily
    {
        SqlConnection myConnection = new SqlConnection();
        SqlConnection myConnection_201 = new SqlConnection();
        public void fn_ppmdaily(string orderdate5, string orderdate6, Form1 frm1)
        {
            //frm1.sendSms("PPM " + orderdate5 + "  정가판매 데이터 생성 시작");
            fn_get_sale_ppm_daily(orderdate5, orderdate6, frm1); //기초 데이터 생성
            frm1.sendSms("PPM  " + orderdate5 + " 정가판매 데이터 생성  완료");
        }

        private void fn_get_sale_ppm_daily(string orderdate5, string orderdate6, Form1 frm1)
        {
            string sql = null;
            string queryString1 = "";

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            SqlCommand command = new SqlCommand(sql, myConnection);
            SqlDataAdapter adapter = new SqlDataAdapter();
           // SqlCommand command = new SqlCommand();
            frm1.Log("PPM 정가판매 데이터 생성 시작");

            string YYYYMM = String.Format("{0:yyyyMM}", Convert.ToDateTime(orderdate5));

           
            try
            {
                //1. 낱개 박스설정 제외한 물품 적용     !!!!!!!!!!!수정
                // 제외 카테고리 적용
                sql = "  INSERT INTO preset_sale_ppm_daily"
                        + "  (orderdate, storeno, ppmidx, qty_jungga, qty_total, qty_discount) "
                        + "  SELECT a.orderdate,  right(a.storeno,4) as storeno, a.ppmidx "
                        + " , SUM(CASE WHEN a.vc_mainvoid='0' THEN a.qty ELSE 0 END)  - SUM(CASE WHEN a.vc_mainvoid='0' and (a.price_discount<>0 OR b.membergrade='13')THEN a.qty ELSE 0 END)  as qty_jungga "
                        + " , SUM(CASE WHEN a.vc_mainvoid='0' THEN a.qty ELSE 0 END) as qty_total "
                        + " , SUM(CASE WHEN a.vc_mainvoid='0' and (a.price_discount<>0 OR b.membergrade='13') THEN a.qty ELSE 0 END) as qty_discount  "
                        + " FROM pos_sale_orderNo_detail_" + YYYYMM + " a WITH (NOLOCK) "
                        + " LEFT JOIN pos_member_sy b WITH (NOLOCK) ON a.memidx=b.memidx "
                        + " LEFT JOIN pos_db_membergrade_sub c WITH (NOLOCK) ON c.gradeNo2=b.memberGrade2 and c.memo='폐기자가' "
                        + " LEFT JOIN pos_product_master_box as d WITH (NOLOCK) on a.ppmidx = d.ppmidx_box and d.vc ='1' "
                        + "    WHERE  a.orderDate >= '" + orderdate5 + "'  and   a.orderDate <=  '" + orderdate6 + "'    "
                        + " AND a.ppmidx in ( "
                        + " select ppmidx from pos_sale_A01_ppm_daily  as a with(nolock) "
                        + "  left join pos_product_master as b with(nolock)  on a.ppmidx = b.idx "
                        + " where   (a.orderdate >= '" + orderdate5 + "'  and a.orderdate <= '" + orderdate6 + "' ) and  b.pdbidx not in ('3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') and  "
                        + " b.pcmidx in (select idx from  pos_category_master where idx not in ('1','4','15','18','23','62','77','83','84','85','86','102','107','106','108','109','155','103','129','130','144','161','164')  and pcgidx not in('32','33') ) "
                        //  + " and b.erp_p_ab = 'A' and  b.erp_recall_ab  <> 'A' and  b.erp_shop_release_date > 1 "
                        + "group by ppmidx "
                        + " ) "
                        //  + " AND a.ppmidx in (SELECT ppmidx FROM pos_sale_A01_ppm_daily WHERE  orderdate >=  '" + orderdate5 + "' and orderdate <= '" + orderdate6 + "'  )"
                        + " and ppmidx_box is null "
                        + " GROUP BY  a.orderdate,a.storeno,a.ppmidx "
                        + " having SUM(CASE WHEN a.vc_mainvoid='0' THEN a.qty ELSE 0 END)  - SUM(CASE WHEN a.vc_mainvoid='0' and (a.price_discount<>0 OR b.membergrade='13')THEN a.qty ELSE 0 END)  > 0 ";
                command = new SqlCommand(sql, myConnection);
                command.CommandTimeout = 0;
                command.CommandText = sql;
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                frm1.sendSms("ppm_daily 박스 제외 쿼리 1:" + ex.Message);
                frm1.Log("ppm_daily 박스 제외 쿼리 1(fn_get_sale_ppm_daily)]:" + ex.Message);
                frm1.Log("박스제외쿼리:" + sql);
                //throw;
            }
           

            //myConnection.Dispose();
            //myConnection.Close();

            //2. 낱개 박스설정 포함한 물품에 대한 낱개코드로 환산
            string queryString = " SELECT a.orderdate, right(a.storeno,4) as storeno, a.ppmidx, d.ppmidx_single  "
                            + " , SUM(CASE WHEN a.vc_mainvoid='0' THEN a.qty * ppmidx_box_ea ELSE 0 END) - SUM(CASE WHEN a.vc_mainvoid='0' and (a.price_discount<>0 OR b.membergrade='13') THEN a.qty * ppmidx_box_ea ELSE 0 END) as qty_jungga "
                            + " , SUM(CASE WHEN a.vc_mainvoid='0' THEN a.qty * ppmidx_box_ea ELSE 0 END) as qty_total "
                            + ", SUM(CASE WHEN a.vc_mainvoid='0' and (a.price_discount<>0 OR b.membergrade='13') THEN a.qty * ppmidx_box_ea ELSE 0 END) as qty_discount  "
                            + " FROM pos_sale_orderNo_detail_" + YYYYMM + " a WITH (NOLOCK) "
                            + " LEFT JOIN pos_member_sy b WITH (NOLOCK) ON a.memidx=b.memidx "
                            + " LEFT JOIN pos_db_membergrade_sub c WITH (NOLOCK) ON c.gradeNo2=b.memberGrade2 and c.memo='폐기자가' "
                            + " LEFT JOIN pos_product_master_box as d WITH (NOLOCK) on a.ppmidx = d.ppmidx_box and d.vc ='1' "
                            + "    WHERE  a.orderDate >= '" + orderdate5 + "'  and   a.orderDate <=  '" + orderdate6 + "'   " // AND d.ppmidx_single in ('26433','15494','21219')   " //cug 2276010 덧글 28> 등록된 물품만
                            + " AND a.ppmidx in ( "
                            + " select ppmidx from pos_sale_A01_ppm_daily  as a with(nolock) "
                            + "  left join pos_product_master as b with(nolock)  on a.ppmidx = b.idx "
                            + " where   (a.orderdate >= '" + orderdate5 + "'  and a.orderdate <= '" + orderdate6 + "' ) and  b.pdbidx not in ('3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') and  "
                            + " b.pcmidx in (select idx from  pos_category_master where idx not in ('1','4','15','18','23','62','77','83','84','85','86','102','107','106','108','109','155','103','129','130','144','161','164') and pcgidx not in('32','33') ) "
                            //    + " and b.erp_p_ab = 'A' and  b.erp_recall_ab  <> 'A' and  b.erp_shop_release_date > 1 "
                            + "group by ppmidx "
                            + " ) "
                             //+ " AND a.ppmidx in (SELECT ppmidx FROM pos_sale_A01_ppm_daily WHERE  orderdate >=  '" + orderdate5 + "' and orderdate <= '" + orderdate6 + "'  )"
                             + " and ppmidx_box is not null  "
                             + "  GROUP BY a.orderdate,  a.storeno,a.ppmidx, d.ppmidx_single "
                             + " 	having SUM(CASE WHEN a.vc_mainvoid='0' THEN a.qty * ppmidx_box_ea ELSE 0 END) - SUM(CASE WHEN a.vc_mainvoid='0' and (a.price_discount<>0 OR b.membergrade='13') THEN a.qty * ppmidx_box_ea ELSE 0 END) > 0 ";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            adapter.SelectCommand.CommandTimeout = 0;
    

            try
            {

                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string orderdate = "";
                string storeno = "";
                string ppmidx_single = "";
                int qty_jungga = 0;
                int qty_total = 0;
                int qty_discount = 0;

                foreach (DataRow dr in rows)
                {
                    orderdate = dr["orderdate"].ToString();
                    storeno = dr["storeno"].ToString();
                    ppmidx_single = dr["ppmidx_single"].ToString().Trim();
                    qty_jungga = Convert.ToInt32(dr["qty_jungga"].ToString());
                    qty_total = Convert.ToInt32(dr["qty_total"].ToString());
                    qty_discount = Convert.ToInt32(dr["qty_discount"].ToString());


                    //기존에 있는 물품인 경우, 설정값 Update 없는 물품인 경우 Insert
                    queryString1 = "IF EXISTS (SELECT * FROM preset_sale_ppm_daily WITH (NOLOCK) WHERE orderdate = '" + orderdate + "' AND storeno ='"+ storeno+ "' AND ppmidx ='" + ppmidx_single +"')"
                                                    + " BEGIN "
                                                    + "     UPDATE preset_sale_ppm_daily "
                                                    + "     SET qty_jungga = qty_jungga + '" + qty_jungga + "', qty_total = qty_total + '" + qty_total + "', qty_discount = qty_discount +'" + qty_discount + "'"
                                                    + "     WHERE orderdate = '" + orderdate + "' AND storeno = '" + storeno + "' AND ppmidx ='" + ppmidx_single + "' "
                                                    + " END"
                                                    + " ELSE"
                                                    + " BEGIN"
                                                    + "     INSERT INTO preset_sale_ppm_daily "
                                                    + "      (orderdate, storeno, ppmidx, qty_jungga, qty_total, qty_discount) "
                                                    + "     VALUES ('" + orderdate + "', '" + storeno + "', '" + ppmidx_single + "', '" + qty_jungga + "', '" + qty_total + "', '" + qty_discount + "') "
                                                    + " END";

          
                    SqlCommand command1 = new SqlCommand(queryString1, myConnection);
                    command1.CommandText = queryString1;
                    command1.ExecuteNonQuery();

                }
              //  frm1.sendSms("NEWPOS_PPM  기초 자료  생성 완료");
                frm1.Log("PPM 정가판매 데이터  생성 완료");
              
            }
            catch (Exception ex)
            {
                frm1.sendSms("박스기초자료오류:" + ex.Message);
                frm1.Log("[박스기초자료오류]:" + ex.Message);
                frm1.Log("박스기초자료쿼리:" + queryString1);
                //throw;
            }
           
            myConnection.Dispose();
            myConnection.Close();
            
            frm1.Log((String.Format("[MsSql 정가판매량 ]:{0}", myConnection.State.ToString())));

        }

    }
}
