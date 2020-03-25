using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Net;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Windows.Forms;

namespace SCM_Auto_baju
{
    class stock_auto_input
    {
        SqlConnection myConnection = new SqlConnection();
        public void fn_stockAuto(string orderdate, string type, string ip,  Form1 frm1)
        {
            fn_stock_auto_input(orderdate, type, ip, frm1); //매장 자동 입고
        }

        private void fn_stock_auto_input(string orderdate, string type, string ip, Form1 frm1)
        {

            frm1.Log((String.Format("NEWPOS 물품 자동입고 시작")));

            string storeNo = "";
            string storeNo_chack = "8000006999";

            String queryString1 = "";
            String queryString4 = "";
            String queryString5 = "";

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            SqlDataAdapter adapter = new SqlDataAdapter();

            string queryString = "select a.idx, a.ppmidx, a.storeNo, a.order_date, a.order_icoopCode, a.order_price, a.order_qty, a.order_qty_reconfirm, a.order_sort1, a.order_sort2, a.storeCheck_qty, a.storeCheck_vc,b.vc_packingBox  "
                        + "  from pos_store_icoopOrder as a  with (nolock)  left outer join pos_product_master as b with (nolock) on a.ppmidx = b.idx  "
                        + "   left outer join pos_store as c with (nolock) on a.storeno = c.storeno  "
                        + "  where a.order_date = '" + orderdate + "'  and a.storeCheck_vc ='0'  and a.ppmidx <> '' and c.vc_autoinput = '1'  order by a.storeNo";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            SqlCommand command = new SqlCommand(queryString, myConnection);

            try
            {
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                //string ppmidx = "";
                int ppmidx_box_ea = 0;
                int amtDiff = 0;
                int countsum = 0;
                int vc_countsum = 0;
                int count_storeNo = 0;

                foreach (DataRow dr in rows)
                {
                    int vc_display = 0;
                    String subject = "";
                    String pcmidx = "";

                    string tmp_ppmidx = dr[1].ToString();
                    storeNo = dr[2].ToString();
                    String tmp_icoopcode = dr[4].ToString();
                    String vc_packingBox = dr[12].ToString();

                    amtDiff = Convert.ToInt32(dr[6].ToString());

                    if (tmp_ppmidx != "")
                    {
                        if (vc_packingBox == "1")
                        {

                            queryString4 = " select ppmidx_single, ppmidx_box_ea   "
                            + "  from pos_product_master_box    "
                            + "   where ppmidx_box = '" + tmp_ppmidx + "' and vc ='1' ";

                            SqlCommand command1 = new SqlCommand(queryString4, myConnection);
                            SqlDataReader reader1 = command1.ExecuteReader();
                            while (reader1.Read())
                            {

                                tmp_ppmidx = reader1["ppmidx_single"].ToString();
                                ppmidx_box_ea = Convert.ToInt32(reader1["ppmidx_box_ea"].ToString());

                                amtDiff = amtDiff * ppmidx_box_ea;
                            }

                            reader1.Close();

                        }

                        queryString5 = " select vc_display,subject,pcmidx   "
                                    + "  from pos_product_master with (nolock)   "
                                    + "   where idx = '" + tmp_ppmidx + "' ";

                        SqlCommand command2 = new SqlCommand(queryString5, myConnection);
                        SqlDataReader reader2 = command2.ExecuteReader();
                        while (reader2.Read())
                        {

                            subject = reader2["subject"].ToString();
                            vc_display = Convert.ToInt32(reader2["vc_display"].ToString());
                            pcmidx = reader2["pcmidx"].ToString();

                        }
                        reader2.Close();

                        if (amtDiff != 0)
                        {
                            if (type == "A") //자동 입고를 진행하였을 경우
                            {
                                queryString1 = " IF NOT EXISTS (SELECT ppmidx from pos_store_stock where storeno = '" + storeNo + "' and ppmidx = '" + tmp_ppmidx + "') "
                                                + " BEGIN "
                                                + "   insert into pos_store_stock (storeNo, orderDate, ppmidx, amt_input, rdate_input, sdate_input, finalUpdate, adminMemidx) values "
                                                + "   ( '" + storeNo + "' , '" + orderdate + "' , '" + tmp_ppmidx + "', '" + amtDiff + "', convert(varchar(10),getdate(),120) , convert(varchar(10),getdate(),120), GETDATE(), 'DEMON1') ;"
                                                + " END "
                                                + " ELSE  IF NOT EXISTS (SELECT ppmidx from pos_store_stock where storeno =  '" + storeNo + "' and ppmidx = '" + tmp_ppmidx + "' and sdate_input is null) "
                                                + " BEGIN "
                                                + "   UPDATE pos_store_stock SET "
                                                + "   amt_input = amt_input +'" + amtDiff + "'"
                                                + "   , rdate_input ='" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"
                                                + "   , vc_stock = '1'"
                                                + "   , finalUpdate = GETDATE()"
                                                + "   ,adminMemidx = 'DEMON1'"
                                                + "   WHERE storeno = '" + storeNo + "'  AND ppmidx = '" + tmp_ppmidx + "'"
                                                + " END "
                                                + " ELSE "
                                                + " BEGIN "
                                                + "   UPDATE pos_store_stock SET "
                                                + "   amt_input = amt_input +'" + amtDiff + "'"
                                                + "   , rdate_input ='" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"
                                                + "   , sdate_input = '" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"  //최초입고일
                                                + "   , vc_stock = '1'"
                                                + "   , finalUpdate = GETDATE()"
                                                + "   ,adminMemidx = 'DEMON1'"
                                                + "   WHERE storeno = '" + storeNo + "'  AND ppmidx = '" + tmp_ppmidx + "'"
                                                + " END ";

                                //Log((String.Format("[in_stock] 쿼리[{0}] : ", queryString1)));
                                command.CommandText = queryString1;
                                command.ExecuteNonQuery();

                                //최초입고일 메인 테이블 추가(2019.09.09)
                                string queryString2 = " IF NOT EXISTS (SELECT ppmidx from pos_product_date where storeno = '" + storeNo.Substring(6, 4) + "' and ppmidx = '" + tmp_ppmidx + "') "
                                               + " BEGIN "
                                               + "   insert into pos_product_date (storeNo, ppmidx,sdate_input) values "
                                               + "   ( '" + storeNo.Substring(6, 4) + "' ,'" + tmp_ppmidx + "',getdate()) "
                                               + " END ";

                                command.CommandText = queryString2;
                                command.ExecuteNonQuery();


                                if (vc_display == 0 && (pcmidx != "77" && pcmidx != "83"))
                                {
                                    queryString1 = "exec usp_pos_product_update_display '" + tmp_ppmidx + "', '" + tmp_icoopcode + "', '" + subject + "' , '1', 'DEMON1', 'System', 'D';";
                                    command.CommandText = queryString1;
                                    command.ExecuteNonQuery();

                                    vc_countsum = vc_countsum + 1;

                                    frm1.Log((String.Format("[vc_input] 쿼리[{0}] : ", queryString1)));
                                }


                            }
                            else
                            {
                                if (System.DateTime.Now.ToString("yyyy-MM-dd").CompareTo(orderdate) >= 0 && amtDiff != 0) //현재날짜가 입고일보다 작거나 같을 경우(선입고 할 경우는 안됨)
                                {
                                    queryString1 = " IF NOT EXISTS (SELECT ppmidx from pos_store_stock where storeno = '" + storeNo + "' and ppmidx = '" + tmp_ppmidx + "') "
                                                 + " BEGIN "
                                                 + "   insert into pos_store_stock (storeNo, orderDate, ppmidx, amt_input, rdate_input, sdate_input, finalUpdate, adminMemidx) values "
                                                 + "   ( '" + storeNo + "' , '" + orderdate + "' , '" + tmp_ppmidx + "', '" + amtDiff + "', convert(varchar(10),getdate(),120) , convert(varchar(10),getdate(),120), GETDATE(), 'DEMON1') ;"
                                                 + " END "
                                                 + " ELSE  IF NOT EXISTS (SELECT ppmidx from pos_store_stock where storeno =  '" + storeNo + "' and ppmidx = '" + tmp_ppmidx + "' and sdate_input is null) "
                                                 + " BEGIN "
                                                 + "   UPDATE pos_store_stock SET "
                                                 + "   amt_input = amt_input +'" + amtDiff + "'"
                                                 + "   , rdate_input ='" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"
                                                 + "   , vc_stock = '1'"
                                                 + "   , finalUpdate = GETDATE()"
                                                 + "   ,adminMemidx = 'DEMON1'"
                                                 + "   WHERE storeno = '" + storeNo + "'  AND ppmidx = '" + tmp_ppmidx + "'"
                                                 + " END "
                                                 + " ELSE "
                                                 + " BEGIN "
                                                 + "   UPDATE pos_store_stock SET "
                                                 + "   amt_input = amt_input +'" + amtDiff + "'"
                                                 + "   , rdate_input ='" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"
                                                 + "   , sdate_input = '" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"  //최초입고일
                                                 + "   , vc_stock = '1'"
                                                 + "   , finalUpdate = GETDATE()"
                                                 + "   ,adminMemidx = 'DEMON1'"
                                                 + "   WHERE storeno = '" + storeNo + "'  AND ppmidx = '" + tmp_ppmidx + "'"
                                                 + " END ";

                                    command.CommandText = queryString1;
                                    command.ExecuteNonQuery();


                                    string queryString2 = " IF NOT EXISTS (SELECT ppmidx from pos_product_date where storeno = '" + storeNo.Substring(6, 4) + "' and ppmidx = '" + tmp_ppmidx + "') "
                                                 + " BEGIN "
                                                 + "   insert into pos_product_date (storeNo, ppmidx,sdate_input) values "
                                                 + "   ( '" + storeNo.Substring(6, 4) + "' ,'" + tmp_ppmidx + "',getdate()) "
                                                 + " END ";
                                              
                                    command.CommandText = queryString2;
                                    command.ExecuteNonQuery();


                                    if (vc_display == 0 && (pcmidx != "77" && pcmidx != "83"))
                                    {

                                        queryString1 = "exec usp_pos_product_update_display '" + tmp_ppmidx + "', '" + tmp_icoopcode + "', '" + subject + "' , '1', 'DEMON1', 'System', 'D';";
                                        command.CommandText = queryString1;
                                        command.ExecuteNonQuery();

                                        frm1.Log((String.Format("[vc_input] 쿼리[{0}] : ", queryString1)));
                                        vc_countsum = vc_countsum + 1;

                                    }

                                }

                                 if ((string.Format(DateTime.Now.ToString("yyyy-MM-dd")) != orderdate) && amtDiff != 0)
                                {

                                    queryString1 = "insert into pos_store_icoopOrder_other (storeNo, order_date, order_date_real, order_icoopCode, order_price,vc_insertType,ppmidx,storeCheck_qty,storeCheck_update)  values "
                                        + "('" + storeNo + "'"
                                    + ",'" + orderdate + "'"
                                    + ",'" + string.Format(DateTime.Now.ToString("yyyy-MM-dd")) + "'"
                                    + ",'" + tmp_icoopcode + "'"
                                    + ",'0'"
                                    + ",'0'"
                                    + ",'" + tmp_ppmidx + "'"
                                    + ",'" + amtDiff + "'"
                                    + ",GETDATE()"
                                    + ");";
                                    // Log((String.Format("[in_other] 쿼리[{0}] : ", queryString1)));
                                    command.CommandText = queryString1;
                                    command.ExecuteNonQuery();

                                }

                            }

                          queryString1 = "insert into pos_store_icoopOrder_log (storeno,orderdate,ppmidx,qty,regdate,adminName,userip)  values "
                          + "('" + storeNo + "'"
                          + ",'" + orderdate + "'"
                          + ",'" + tmp_ppmidx + "'"
                          + ",'" + amtDiff + "'"
                          + ",GETDATE()"
                          + ",'DEMON1'"
                          + ",'" + ip + "'"
                          + ");";
                            //Log((String.Format("[in_other_log] 쿼리[{0}] : ", queryString1)));
                            command.CommandText = queryString1;
                            command.ExecuteNonQuery();

                            queryString1 = "update pos_store_icoopOrder set storeCheck_qty = '" + dr[6].ToString() + "', storeCheck_vc = '1' , storeCheck_update = getdate()  where storeno = '" + storeNo + "' and order_date  = '" + orderdate + "' and idx =  '" + dr[0].ToString() + "';";
                            //Log((String.Format("[up_other] 쿼리[{0}] : ", queryString1)));    
                            command.CommandText = queryString1;
                            command.ExecuteNonQuery();

                            countsum = countsum + 1;

                            if (storeNo != storeNo_chack) //
                            {
                                queryString1 = "update pos_store_icoopOrder_status set total_count_storeCheck = (select count(storecheck_qty) as total_count_storeCheck from pos_store_icoopOrder where storeno = '" + storeNo_chack + "' and order_date ='" + orderdate + "' and storeCheck_vc = '1') "
                                    + "  where storeno ='" + storeNo_chack + "' and order_date ='" + orderdate + "' ";
                                //Log((String.Format("[up_other_status] 쿼리[{0}] : ", queryString1)));
                                command.CommandText = queryString1;
                                command.ExecuteNonQuery();

                                if (storeNo_chack != "8000006999")
                                {
                                    count_storeNo = count_storeNo + 1;
                                }
                            }

                            storeNo_chack = storeNo;

                        }

                        //frm1.Log((String.Format("[AUTO stock] - ppmidx=[{0}] / storeno=[{1}] / orderdate =[{2}]", tmp_ppmidx, storeNo, orderdate)));
                    }

                }
                queryString1 = "update pos_store_icoopOrder_status set total_count_storeCheck = (select count(storecheck_qty) as total_count_storeCheck from pos_store_icoopOrder where storeno = '" + storeNo + "' and order_date ='" + orderdate + "' and storeCheck_vc = '1') "
                                   + "  where storeno ='" + storeNo + "' and order_date ='" + orderdate + "' ";
                command.CommandText = queryString1;
                command.ExecuteNonQuery();

                count_storeNo = count_storeNo + 1;

                frm1.sendSms("자동입고 완료 \n[" + orderdate + "] [" + count_storeNo + "]매장,\n [" + countsum + "]건,  [" + vc_countsum + "] 보임 ");
                frm1.Log((String.Format("NEWPOS 물품 자동입고 종료")));
            }
            catch (SqlException ex)
            {
                StringBuilder errorMessages = new StringBuilder();
                for (int i = 0; i < ex.Errors.Count; i++)
                {
                    errorMessages.Append("Index #" + i + " || " +
                        "Message: " + ex.Errors[i].Message + " || " +
                        "LineNumber: " + ex.Errors[i].LineNumber + " || " +
                        "Source: " + ex.Errors[i].Source + " || " +
                        "Procedure: " + ex.Errors[i].Procedure + ";");
                    //쿼리정보도
                }

                frm1.Log(errorMessages.ToString());
                frm1.Log((String.Format("[에러] 쿼리[{0}] : ", queryString1)));
                frm1.sendSms("[" + orderdate + "] 자동입고 에러");
            }

            myConnection.Dispose();
            myConnection.Close();

        }

    }
}
