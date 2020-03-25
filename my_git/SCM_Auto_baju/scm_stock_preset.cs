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
    class scm_stock_preset
    {
        SqlConnection myConnection = new SqlConnection();
        SqlConnection myConnection_201 = new SqlConnection();
        MySqlConnection MYconn = new MySqlConnection();
        public void fn_scmstock(string orderdate1, string orderdate2, Form1 frm1)
        {
            frm1.sendSms("NEWPOS_SCM " + orderdate1 + "  데이터 생성 시작");
            fn_get_stock_preset(orderdate1, orderdate2, frm1); //SCM 기초자료 생성
           // fn_get_stock_preset_jungga(orderdate1, orderdate2, frm1); //정가판매량 업데이트
           // fn_get_stock_preset_panmae(orderdate1, orderdate2, frm1); //판매예측값 업데이트
            //fn_get_stock_preset_stock(orderdate1, orderdate2, frm1); //재고 업데이트
            frm1.sendSms("NEWPOS_SCM  " + orderdate1 + " 데이터 생성  완료");
        }

        private void fn_get_stock_preset(string orderdate1, string orderdate2, Form1 frm1)
        {

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

            int totalcount = 0;
            int pagecount = 0;
            int i;

            string YYYYMM = "";
            YYYYMM = String.Format("{0:yyyyMM}", Convert.ToDateTime(orderdate1));

            frm1.Log("NEWPOS_SCM 기준데이터 생성 시작");

            string queryString4 = "  select count(g.storeno) from ( SELECT t1.storeno as storeno, t1.sdate,  t1.m_code as m_code, t1.product_code, t4.storeSubject as storeSubject,  t1.ppmidx,t3.subject, t1.reg_gb,"
                                    + "     (case when (t1.product_code <> '') THEN '물품' ELSE '그룹' END) as group_yn,  isnull(gg.group_cd,'') as group_cd,isnull( gg.group_nm,'') as group_nm,  t1.scm_gb,   "
                                    + " 	 (case when ( t1.stock_only_ab = 'A') THEN 'Y' when ( t1.stock_only_ab = 'B') THEN 'N' ELSE '' END) as  stock_only_ab, "
                                    + " 	 (case when ( t1.scm_susik = 'D') THEN '기본' when ( t1.scm_susik = 'B') THEN '묶음' ELSE '' END) as  scm_susik"
                                    + "	 , t1.amt_bundle, t5.subject3 as pdbsubject ,t5.idx as pdbidx , t7.subject as pcgsubject ,t7.idx as pcgidx,t6.subject as pcmsubject,t6.idx as pcmidx"
                                    + "	 ,group_susik,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d"
                                    + "	  ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d,amt_safe_max,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1"
                                    + "	  ,amt_adjust1,amt_adjust2,amt_adjust3,amt_adjust4,amt_adjust5,amt_adjust6,amt_adjust7,t1.vc, t3.erp_p_ab,t3.erp_netshow_ab"
                                       + "      , (SUM(ISNULL(t8.amt_sale, 0)) ) as panmae "
                                    + "     , SUM(ISNULL(t9.amt_result,0)) as amt_pre ,  SUM(ISNULL(t9.amt_input,0)) as amt_input ,(case when (sum(t9.amt_result) <= 0) THEN 1 ELSE 0 END) as amt_soldout  "
                                    + "     ,SUM(ISNULL(t10.qty_jungga, 0)) as qty_jungga "
                                    + "	 FROM webdb_se.dbo.stock_preset4_2019 as t1 with (nolock) "
                                    + "     LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx"
                                    + "	 LEFT OUTER JOIN pos_erp_goods_group as gg with (nolock) On t1.ppmidx = gg.product_ppmidx "
                                    + "	 LEFT OUTER JOIN pos_db_BrandClass as t5 with (nolock) On t5.idx = t3.pdbidx "
                                    + "	 LEFT OUTER JOIN pos_category_master as t6 with (nolock) On t6.idx = t3.pcmidx "
                                    + "	 LEFT OUTER JOIN pos_category_group as t7 with (nolock) on t7.idx = t6.pcgidx"
                                    // + "     LEFT OUTER JOIN webdb_se.dbo.preset_sale_data_2019 as t8 with(nolock) on convert(varchar(10),t1.ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN preset_sale_data as t8 with(nolock) on convert(varchar(10),t1.ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN pos_store_stock_" + YYYYMM + " as t9 with(nolock) on t9.orderdate = t1.sdate and right(t9.storeno,4) = t1.storeno and t9.ppmidx = t1.ppmidx "
                                    + "     LEFT OUTER JOIN preset_sale_ppm_daily as t10 with(nolock) on t10.ppmidx = t1.ppmidx  and t10.orderdate = t1.sdate and t10.storeno = t1.storeno "
                                    + "	  INNER JOIN pos_store as t4 ON t1.storeno=right(t4.storeno,4) and t4.vc_businessStatus='1' "
                                    + "	  WHERE (t1.sdate >=  '" + orderdate1 + "' and t1.sdate <=  '" + orderdate2 + "' )   "
                                    + "	  group by  t1.sdate,t1.storeno , t1.m_code , t4.storeSubject ,  t1.product_code, t3.subject, t1.reg_gb,t1.product_code,"
                                    + "	   t1.scm_gb, t1.stock_only_ab, t1.scm_susik, t1.amt_bundle, t5.subject3  ,t5.idx , t7.subject  ,t7.idx,t6.subject,t6.idx,"
                                    + "	  gg.group_cd, gg.group_nm,t1.ppmidx,group_susik,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d"
                                    + "     ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d,amt_safe_max,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1"
                                    + "	  ,amt_adjust1,amt_adjust2,amt_adjust3,amt_adjust4,amt_adjust5,amt_adjust6,amt_adjust7,t1.vc, t3.erp_p_ab,t3.erp_netshow_ab"
                                    + " UNION ALL  "
                                    + "  SELECT  distinct t1.storeno as storeno, t1.sdate,   t1.m_code as m_code, ''  as  product_code,  t4.storeSubject as storeSubject,   null  as ppmidx, ''  as subject, t1.reg_gb,  "
                                    + "  (case when (''='') THEN '그룹' ELSE '물품' END) as group_yn, gg.group_cd, gg.group_nm, "
                                    + "  (case when (group_ab = 'A') then 'Y' else 'N' END) as scm_gb,"
                                    + "  (case when ( t1.stock_only_ab = 'A') THEN 'Y' when ( t1.stock_only_ab = 'B') THEN 'N' ELSE '' END) as  stock_only_ab,  "
                                    + "	  ''  as scm_susik,  ''  as amt_bundle,t5.subject3 as pdbsubject ,t5.idx as pdbidx , t7.subject as pcgsubject ,t7.idx as pcgidx,t6.subject as pcmsubject,t6.idx as pcmidx, "
                                    + "	  ''  as  group_susik,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d "
                                    + "  ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d, 0 as amt_safe_max,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1 "
                                    + "  ,amt_adjust1,amt_adjust2,0 as  amt_adjust3,amt_adjust4,0 as  amt_adjust5,amt_adjust6,amt_adjust7,t1.vc, t3.erp_p_ab,t3.erp_netshow_ab"
                                      + " , (SUM(ISNULL(t8.amt_sale, 0)) ) as panmae  "
                                    + ", SUM(ISNULL(t9.amt_result, 0)) as amt_pre ,  SUM(ISNULL(t9.amt_input, 0)) as amt_input ,(case when(sum(t9.amt_result) <= 0) THEN 1 ELSE 0 END) as amt_soldout "
                                    + " , SUM(ISNULL(t10.qty_jungga, 0)) as qty_jungga "
                                    + "	   FROM stock_preset4_group as t1  "
                                    + "   LEFT OUTER JOIN pos_erp_goods_group as gg On t1.group_cd = gg.group_cd  "
                                    + "   LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t3.icoop_code = gg.product_cd  "
                                    + "   LEFT OUTER JOIN pos_db_BrandClass as t5 with (nolock) On t5.idx = t3.pdbidx  "
                                    + "   LEFT OUTER JOIN pos_category_master as t6 with (nolock) On t6.idx = t3.pcmidx "
                                    + "   LEFT OUTER JOIN pos_category_group as t7 with (nolock) on t7.idx = t6.pcgidx  "
                                    //+ "     LEFT OUTER JOIN webdb_se.dbo.preset_sale_data_2019 as t8 with(nolock) on convert(varchar(10),gg.product_ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN preset_sale_data as t8 with(nolock) on convert(varchar(10),gg.product_ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN pos_store_stock_" + YYYYMM + "  as t9 with(nolock) on t9.orderdate = t1.sdate and right(t9.storeno,4) = t1.storeno and t9.ppmidx = gg.product_ppmidx "
                                    + "     LEFT OUTER JOIN preset_sale_ppm_daily as t10 with(nolock) on t10.ppmidx = gg.product_ppmidx  and t10.orderdate = t1.sdate and t10.storeno = t1.storeno "
                                    + "   INNER JOIN pos_store as t4 ON t1.storeno=right(t4.storeno,4) and t4.vc_businessStatus='1'  "
                                    + "   WHERE (t1.sdate >=  '" + orderdate1 + "'  and t1.sdate <=  '" + orderdate2 + "' )  "
                                    + "     group by t1.storeno ,t1.sdate, t1.m_code , t4.storeSubject ,   t3.subject, t1.reg_gb, "
                                    + "     t1.stock_only_ab,  t5.subject3  ,t5.idx , t7.subject  ,t7.idx,t6.subject,t6.idx,group_ab,gg.group_cd, gg.group_nm "
                                    + "	 ,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d "
                                    + "     ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1 "
                                    + "     ,amt_adjust1,amt_adjust2,amt_adjust4,amt_adjust6,amt_adjust7,t1.vc, t3.erp_p_ab,t3.erp_netshow_ab "
                                    + "     ) as g";

            SqlCommand command4 = new SqlCommand(queryString4, myConnection);
            SqlDataReader reader4 = command4.ExecuteReader();
            while (reader4.Read())
            {
                totalcount = Convert.ToInt32(reader4[0].ToString()); //ppmidx

            }
            reader4.Close();

            myConnection.Dispose();
            myConnection.Close();
            frm1.Log("NEWPOS_SCM 자료 생성 개수(" + totalcount + ")");
            //frm1.Log("NEWPOS_SCM 쿼리(" + queryString4 + ")");
            pagecount = totalcount / 50000;
            pagecount = pagecount + 1;

            

            for (i = 1; i <= pagecount; i++)
            {

                myConnection_201 = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("azure");
                string queryString1 = "";
                string queryString3 = "";
                SqlDataAdapter adapter = new SqlDataAdapter();
                SqlDataAdapter adapter1 = new SqlDataAdapter();

                //frm1.sendSms("NEWPOS_SCM 자료 생성 시작(" + String.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now.AddDays(0).ToString()) + ")("+ i + "/"+pagecount+")");


                var table_ex = new DataTable();


                queryString3 = "SELECT TOP 0 * FROM scm_stock_preset ";
                adapter1.SelectCommand = new SqlCommand(queryString3, myConnection_201);
                SqlCommand command2 = new SqlCommand(queryString3, myConnection_201);

                adapter1.Fill(table_ex);


                myConnection_201.Dispose();
                myConnection_201.Close();
                myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");

                //step1. 바코등 등록 및 수정 대상물품 조회
                string queryString = "SELECT t1.storeno as storeno, t1.sdate,  t1.m_code as m_code, t1.product_code, t4.storeSubject as storeSubject, convert(varchar(10),t1.ppmidx) as ppmidx,t3.subject, t1.reg_gb,"
                                    + "  (case when (t1.product_code <> '') THEN '물품' ELSE '그룹' END) as group_yn,  isnull(gg.group_cd,'') as group_cd,isnull( gg.group_nm,'') as group_nm, (case when (t1.scm_gb =  '0') THEN '' ELSE t1.scm_gb END) as scm_gb  ,   "
                                    + " 	 (case when ( t1.stock_only_ab = 'A') THEN 'Y' when ( t1.stock_only_ab = 'B') THEN 'N' ELSE '' END) as  stock_only_ab, "
                                    + " 	 (case when ( t1.scm_susik = 'D') THEN '기본' when ( t1.scm_susik = 'B') THEN '묶음' ELSE '' END) as  scm_susik"
                                    + "	,isnull(t1.amt_bundle,0) as amt_bundle, t5.subject3 as pdbsubject ,t5.idx as pdbidx , t7.subject as pcgsubject ,t7.idx as pcgidx,t6.subject as pcmsubject,t6.idx as pcmidx"
                                    + "	,group_susik,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d"
                                    + "	  ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d,amt_safe_max,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1"
                                    + "	  ,amt_adjust1,amt_adjust2,amt_adjust3,amt_adjust4,amt_adjust5,amt_adjust6,amt_adjust7,t1.vc, isnull(t3.erp_p_ab,'') as  erp_p_ab,  isnull(t3.erp_netshow_ab,'') as erp_netshow_ab "
                                    + "      , (SUM(ISNULL(t8.amt_sale, 0)) ) as panmae "
                                    + "     , SUM(ISNULL(t9.amt_result,0)) as amt_pre ,  SUM(ISNULL(t9.amt_input,0)) as amt_input ,(case when (sum(t9.amt_result) <= 0) THEN 1 ELSE 0 END) as amt_soldout  "
                                    + "     ,SUM(ISNULL(t10.qty_jungga, 0)) as qty_jungga "
                                    + "	 FROM webdb_se.dbo.stock_preset4_2019 as t1 with (nolock) "
                                    + "     LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx "
                                    + "	 LEFT OUTER JOIN pos_erp_goods_group as gg with (nolock) On t1.ppmidx = gg.product_ppmidx "
                                    + "	 LEFT OUTER JOIN pos_db_BrandClass as t5 with (nolock) On t5.idx = t3.pdbidx "
                                    + "	 LEFT OUTER JOIN pos_category_master as t6 with (nolock) On t6.idx = t3.pcmidx "
                                    + "	 LEFT OUTER JOIN pos_category_group as t7 with (nolock) on t7.idx = t6.pcgidx"
                                    + "     LEFT OUTER JOIN preset_sale_data as t8 with(nolock) on convert(varchar(10),t1.ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                  //  + "     LEFT OUTER JOIN webdb_se.dbo.preset_sale_data_2019 as t8 with(nolock) on convert(varchar(10),t1.ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN pos_store_stock_"+YYYYMM+" as t9 with(nolock) on t9.orderdate = t1.sdate and right(t9.storeno,4) = t1.storeno and t9.ppmidx = t1.ppmidx "
                                    + "     LEFT OUTER JOIN preset_sale_ppm_daily as t10 with(nolock) on t10.ppmidx = t1.ppmidx  and t10.orderdate = t1.sdate and t10.storeno = t1.storeno "
                                    + "	  INNER JOIN pos_store as t4 ON t1.storeno=right(t4.storeno,4) and t4.vc_businessStatus='1' "
                                    + "	  WHERE (t1.sdate >=  '" + orderdate1 + "' and t1.sdate <=  '" + orderdate2 + "' )    "
                                    + "	  group by  t1.sdate,t1.storeno , t1.m_code , t4.storeSubject ,  t1.product_code, t3.subject, t1.reg_gb,t1.product_code,"
                                    + "	   t1.scm_gb, t1.stock_only_ab, t1.scm_susik, t1.amt_bundle, t5.subject3  ,t5.idx , t7.subject  ,t7.idx,t6.subject,t6.idx,"
                                    + "	  gg.group_cd, gg.group_nm,t1.ppmidx,group_susik,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d"
                                    + ",   amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d,amt_safe_max,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1"
                                    + "	  ,amt_adjust1,amt_adjust2,amt_adjust3,amt_adjust4,amt_adjust5,amt_adjust6,amt_adjust7,t1.vc,t3.erp_p_ab,t3.erp_netshow_ab"
                                    + "	  UNION ALL  "
                                    + " SELECT  distinct t1.storeno as storeno, t1.sdate,   t1.m_code as m_code, ''  as  product_code,  t4.storeSubject as storeSubject,   ''  as ppmidx, ''  as subject, t1.reg_gb,  "
                                    + "	  (case when (''='') THEN '그룹' ELSE '물품' END) as group_yn, gg.group_cd, gg.group_nm, "
                                    + " (case when (group_ab = 'A') then 'Y' else 'N' END) as scm_gb, "
                                    + " (case when ( t1.stock_only_ab = 'A') THEN 'Y' when ( t1.stock_only_ab = 'B') THEN 'N' ELSE '' END) as  stock_only_ab,  "
                                    + "	    ''  as scm_susik,  ''  as amt_bundle,t5.subject3 as pdbsubject ,t5.idx as pdbidx , t7.subject as pcgsubject ,t7.idx as pcgidx,t6.subject as pcmsubject,t6.idx as pcmidx, "
                                    + "	  ''  as  group_susik,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d "
                                    + " ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d, 0 as amt_safe_max,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1 "
                                    + "  ,amt_adjust1,amt_adjust2,0 as  amt_adjust3,amt_adjust4,0 as  amt_adjust5,amt_adjust6,amt_adjust7,t1.vc, isnull(t3.erp_p_ab,'') as  erp_p_ab,  isnull(t3.erp_netshow_ab,'') as erp_netshow_ab "
                                    + " , (SUM(ISNULL(t8.amt_sale, 0)) ) as panmae  "
                                    + ", SUM(ISNULL(t9.amt_result, 0)) as amt_pre ,  SUM(ISNULL(t9.amt_input, 0)) as amt_input ,(case when(sum(t9.amt_result) <= 0) THEN 1 ELSE 0 END) as amt_soldout "
                                    + " , SUM(ISNULL(t10.qty_jungga, 0)) as qty_jungga "
                                    + "	   FROM stock_preset4_group as t1  "
                                    + "LEFT OUTER JOIN pos_erp_goods_group as gg On t1.group_cd = gg.group_cd  "
                                    + "   LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t3.icoop_code = gg.product_cd  "
                                    + "   LEFT OUTER JOIN pos_db_BrandClass as t5 with (nolock) On t5.idx = t3.pdbidx  "
                                    + "   LEFT OUTER JOIN pos_category_master as t6 with (nolock) On t6.idx = t3.pcmidx "
                                    + "   LEFT OUTER JOIN pos_category_group as t7 with (nolock) on t7.idx = t6.pcgidx  "
                                    //+ "     LEFT OUTER JOIN webdb_se.dbo.preset_sale_data_2019 as t8 with(nolock) on convert(varchar(10),gg.product_ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN preset_sale_data as t8 with(nolock) on convert(varchar(10),gg.product_ppmidx) = t8.ppmidx and t1.sdate = t8.sdate and t1.storeno = t8.storeno "
                                    + "     LEFT OUTER JOIN pos_store_stock_" + YYYYMM + "  as t9 with(nolock) on t9.orderdate = t1.sdate and right(t9.storeno,4) = t1.storeno and t9.ppmidx = gg.product_ppmidx "
                                    + "     LEFT OUTER JOIN preset_sale_ppm_daily as t10 with(nolock) on t10.ppmidx = gg.product_ppmidx  and t10.orderdate = t1.sdate and t10.storeno = t1.storeno "
                                    + "   INNER JOIN pos_store as t4 ON t1.storeno=right(t4.storeno,4) and t4.vc_businessStatus='1'  "
                                    + "WHERE (t1.sdate >=  '" + orderdate1 + "'  and t1.sdate <=  '" + orderdate2 + "' )   "
                                    + "     group by t1.storeno ,t1.sdate, t1.m_code , t4.storeSubject ,   t3.subject, t1.reg_gb, "
                                    + "   t1.stock_only_ab,  t5.subject3  ,t5.idx , t7.subject  ,t7.idx,t6.subject,t6.idx,group_ab,gg.group_cd, gg.group_nm "
                                    + "	,amt_balju_pre,amt_balju,amt_sale_d3,amt_sale_d2,amt_sale_d1,amt_sale_d "
                                     + "  ,amt_pda_d3,amt_pda_d2,amt_pda_d1,amt_safe_d,amt_magam,amt_input_d3,amt_input_d2,amt_input_d1 "
                                    + ",amt_adjust1,amt_adjust2,amt_adjust4,amt_adjust6,amt_adjust7,t1.vc,t3.erp_p_ab,t3.erp_netshow_ab "
                                    + " ORDER BY t1.sdate,t1.storeno, group_cd desc, product_code"
                                   + " offset(" + i + " - 1) * 50000 rows"
                                    + " Fetch next 50000 rows only";

                adapter.SelectCommand = new SqlCommand(queryString, myConnection);
                adapter.SelectCommand.CommandTimeout = 0;
                SqlCommand command = new SqlCommand(queryString, myConnection);

              

                try
                {

                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable table = ds.Tables[0];
                    DataRowCollection rows = table.Rows;

                    myConnection.Dispose();
                    myConnection.Close();
                    myConnection_201 = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("azure");

                    // int i = 0;

                    foreach (DataRow dr in rows)
                    {

                        var row_ex = table_ex.NewRow();

                        row_ex["orderdate"] = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime(dr["sdate"].ToString()));
                        row_ex["storeno"] = dr["storeno"].ToString();
                        row_ex["m_code"] = dr["m_code"].ToString();
                        row_ex["ppmidx"] = dr["ppmidx"].ToString();
                        row_ex["product_code"] = dr["product_code"].ToString();
                        row_ex["storeSubject"] = dr["storeSubject"].ToString();
                        row_ex["subject"] = dr["subject"].ToString();
                        row_ex["reg_gb"] = dr["reg_gb"].ToString();

                        row_ex["group_yn"] = dr["group_yn"].ToString();
                        row_ex["group_cd"] = dr["group_cd"].ToString();
                        row_ex["group_nm"] = dr["group_nm"].ToString();
                        row_ex["scm_gb"] = dr["scm_gb"].ToString();

                        row_ex["stock_only_ab"] = dr["stock_only_ab"].ToString();
                        row_ex["scm_susik"] = dr["scm_susik"].ToString();
                        row_ex["amt_bundle"] = Convert.ToInt32(dr["amt_bundle"].ToString());
                        row_ex["pdbsubject"] = dr["pdbsubject"].ToString();
                        row_ex["pdbidx"] = Convert.ToInt32(dr["pdbidx"].ToString());
                        row_ex["pcgsubject"] = dr["pcgsubject"].ToString();
                        row_ex["pcgidx"] = Convert.ToInt32(dr["pcgidx"].ToString());
                        row_ex["pcmsubject"] = dr["pcmsubject"].ToString();
                        row_ex["pcmidx"] = Convert.ToInt32(dr["pcmidx"].ToString());
                        row_ex["group_susik"] = dr["group_susik"].ToString();

                        row_ex["amt_balju_pre"] = Convert.ToInt32(dr["amt_balju_pre"].ToString());
                        row_ex["amt_balju"] = Convert.ToInt32(dr["amt_balju"].ToString());

                        row_ex["amt_sale_d3"] = Convert.ToInt32(dr["amt_sale_d3"].ToString());
                        row_ex["amt_sale_d2"] = Convert.ToInt32(dr["amt_sale_d2"].ToString());
                        row_ex["amt_sale_d1"] = Convert.ToInt32(dr["amt_sale_d1"].ToString());
                        row_ex["amt_sale_d"] = Convert.ToInt32(dr["amt_sale_d"].ToString());

                        row_ex["amt_pda_d3"] = Convert.ToInt32(dr["amt_pda_d3"].ToString());
                        row_ex["amt_pda_d2"] = Convert.ToInt32(dr["amt_pda_d2"].ToString());
                        row_ex["amt_pda_d1"] = Convert.ToInt32(dr["amt_pda_d1"].ToString());

                        row_ex["amt_safe_d"] = Convert.ToInt32(dr["amt_safe_d"].ToString());
                        row_ex["amt_safe_max"] = Convert.ToInt32(dr["amt_safe_max"].ToString());

                        row_ex["amt_magam"] = Convert.ToInt32(dr["amt_magam"].ToString());

                        row_ex["amt_input_d3"] = Convert.ToInt32(dr["amt_input_d3"].ToString());
                        row_ex["amt_input_d2"] = Convert.ToInt32(dr["amt_input_d2"].ToString());
                        row_ex["amt_input_d1"] = Convert.ToInt32(dr["amt_input_d1"].ToString());

                        row_ex["amt_adjust1"] = Convert.ToInt32(dr["amt_adjust1"].ToString());
                        row_ex["amt_adjust2"] = Convert.ToInt32(dr["amt_adjust2"].ToString());
                        row_ex["amt_adjust3"] = Convert.ToInt32(dr["amt_adjust3"].ToString());
                        row_ex["amt_adjust4"] = Convert.ToInt32(dr["amt_adjust4"].ToString());
                        row_ex["amt_adjust5"] = Convert.ToInt32(dr["amt_adjust5"].ToString());
                        row_ex["amt_adjust6"] = Convert.ToInt32(dr["amt_adjust6"].ToString());
                        row_ex["amt_adjust7"] = Convert.ToInt32(dr["amt_adjust7"].ToString());

                        row_ex["vc"] = Convert.ToInt32(dr["vc"].ToString());

                        row_ex["erp_p_ab"] = dr["erp_p_ab"].ToString();
                        row_ex["erp_netshow_ab"] = dr["erp_netshow_ab"].ToString();

                        row_ex["amt_panmae"] = Convert.ToInt32(dr["panmae"].ToString());
                        row_ex["amt_pre"] = Convert.ToInt32(dr["amt_pre"].ToString());
                        row_ex["amt_input"] = Convert.ToInt32(dr["amt_input"].ToString());
                        row_ex["amt_soldout"] = Convert.ToInt32(dr["amt_soldout"].ToString());
                        row_ex["amt_jungga"] = Convert.ToInt32(dr["qty_jungga"].ToString());

                        table_ex.Rows.Add(row_ex);
                     
                    }
                    using (var bulk = new SqlBulkCopy(myConnection_201))
                    {
                        bulk.DestinationTableName = "scm_stock_preset";
                        bulk.BulkCopyTimeout = 0;
                        bulk.WriteToServer(table_ex);
                    }

                    // frm1.sendSms("NEWPOS_SCM 자료 생성 완료(" + String.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now.AddDays(0).ToString()) + ")(" + i + "/" + pagecount + ")");
                    frm1.Log("NEWPOS_SCM 자료 생성 완료(" + i + "/" + pagecount + ")");
                //frm1.Log("NEWPOS_SCM 자료 생성 완료(" + queryString + "");
                    myConnection_201.Dispose();
                    myConnection_201.Close();

                  
                }
                catch (Exception ex)
                {
                    frm1.Log("[자료 생성)]:" + ex.Message);
                    frm1.Log("자료 생성 쿼리:" + queryString1);

                }

            }


           if (pagecount +1  != i)
            {
                frm1.sendSms("NEWPOS_SCM 기준데이터 오류(" + String.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now.AddDays(0).ToString()) + ") 오류 (1/4)");
                frm1.Log("날짜" + orderdate1 + "~" + orderdate2 + "");
            }
            else
            {
                frm1.sendSms("NEWPOS_SCM 기준데이터 완료(" + String.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now.AddDays(0).ToString()) + ") 완료");
                frm1.Log("NEWPOS_SCM 자료 생성 완료(" + i + "/" + pagecount + ")");
                frm1.Log("날짜" + orderdate1 + "~" + orderdate2 + "");
            }

        }


        private void fn_get_stock_preset_jungga(string orderdate1, string orderdate2, Form1 frm1)
        {
            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            string queryString1 = "";
            SqlDataAdapter adapter = new SqlDataAdapter();

            //frm1.sendSms("NEWPOS_SCM 정가 판매 생성 시작");
            frm1.Log("NEWPOS_SCM 정가 판매  생성 시작");

            string queryString = "select  right(a.storeno,4) as storeno, a.ppmidx, orderdate as ckorderdate,( SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0)) ) as jungga  "
                             + " from pos_sale_A01_ppm_daily as a  with (nolock)   "
                              + " 	inner  join  webdb_se.dbo.stock_preset4_2019 as b with (nolock) on   a.orderdate = b.sdate and right(a.storeno,4) = b.storeno and a.ppmidx = b.ppmidx  "
                             + "  where b.sdate >=  '" + orderdate1 + "' and b.sdate <= '" + orderdate2 + "' "
                             + "   AND b.ppmidx IN (SELECT idx FROM pos_product_master as a with (nolock) INNER JOIN preset_base_product as b with (nolock) ON a.icoop_code = b.product_code WHERE b.vc = '1' )"
                             + "  group by a.storeno, a.ppmidx,orderdate  "
                             + "  HAVING SUM(ISNULL(qty_total,0)) - SUM(ISNULL(qty_discount,0))  > 0 "
                             +" order by orderdate ";
            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            adapter.SelectCommand.CommandTimeout = 0;
            SqlCommand command = new SqlCommand(queryString, myConnection);


            try
            {

                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                string ckorderdate = "";
                int jungga = 0;

                myConnection.Dispose();
                myConnection.Close();
                myConnection_201 = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("azure");


                foreach (DataRow dr in rows)
                {
                    storeno = dr["storeno"].ToString();
                    ckorderdate = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime(dr["ckorderdate"].ToString()));
                    ppmidx = dr["ppmidx"].ToString();
                    jungga = Convert.ToInt32(dr["jungga"].ToString());


                    queryString1 = "   UPDATE scm_stock_preset SET "
                                                + "   amt_jungga = '" + jungga + "'"
                                                + "   WHERE orderdate = '" + ckorderdate + "' and storeno = '" + storeno + "'  AND   ppmidx = '" + ppmidx + "'";

                    SqlCommand command1 = new SqlCommand(queryString1, myConnection_201);
                    command1.CommandText = queryString1;
                    command1.ExecuteNonQuery();

                }
                frm1.sendSms("NEWPOS_SCM 정가 판매  생성 완료(2/4)");
                frm1.Log("NEWPOS_SCM 정가 판매 생성 완료");
                myConnection_201.Dispose();
                myConnection_201.Close();
            }
            catch (Exception ex)
            {
                frm1.Log("[정가 판매]:" + ex.Message);
                frm1.Log("정가 판매쿼리:" + queryString1);

            }
        }


        private void fn_get_stock_preset_panmae(string orderdate1, string orderdate2, Form1 frm1)
        {

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            string queryString1 = "";
            SqlDataAdapter adapter = new SqlDataAdapter();

            // frm1.sendSms("NEWPOS_SCM  판매예측 생성 시작");
            frm1.Log("NEWPOS_SCM 판매예측  생성 시작");


            string queryString = "select  storeno, ppmidx, sdate, ( SUM(ISNULL(amt_sale,0)) ) as panmae   "
                                 + " from webdb_se.dbo.preset_sale_data_2019 with (nolock)where sdate >=  '" + orderdate1 + "' and sdate <= '" + orderdate2 + "'  "
                          //       + " from preset_sale_data with (nolock) where sdate >=  '" + orderdate1 + "' and sdate <= '" + orderdate2 + "'  "
                             + " group by storeno, ppmidx , sdate  "
                             + " having SUM(ISNULL(amt_sale,0)) <> 0  "
                             +" order by sdate ";


            //AND CONVERT(Varchar(10),a.regdate,120) = CONVERT(Varchar(10),getdate(),120)";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            adapter.SelectCommand.CommandTimeout = 0;
            SqlCommand command = new SqlCommand(queryString, myConnection);


            try
            {

                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                string cksdate = "";
                int panmae = 0;

                myConnection.Dispose();
                myConnection.Close();
                myConnection_201 = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("azure");


                foreach (DataRow dr in rows)
                {
                    storeno = dr["storeno"].ToString();
                    cksdate = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime(dr["sdate"].ToString()));
                    ppmidx = dr["ppmidx"].ToString();
                    panmae = Convert.ToInt32(dr["panmae"].ToString());


                    //D-6, D-3 실행하므로 기존에 있는 물품인 경우, 설정값 Update 없는 물품인 경우 Insert
                    queryString1 = "   UPDATE scm_stock_preset SET "
                                                + "   amt_panmae = '" + panmae + "'"
                                                + "   WHERE orderdate = '" + cksdate + "' and storeno = '" + storeno + "'  AND  ppmidx = '" + ppmidx + "'";

                    //Log((String.Format("[in_stock] 쿼리[{0}] : ", queryString1)));

                    SqlCommand command1 = new SqlCommand(queryString1, myConnection_201);
                    command1.CommandText = queryString1;
                    command1.ExecuteNonQuery();

                }
                frm1.sendSms("NEWPOS_SCM 판매예측  생성 완료(3/4)");
                frm1.Log("NEWPOS_SCM 판매예측  생성 완료");
                myConnection_201.Dispose();
                myConnection_201.Close();
            }
            catch (Exception ex)
            {
                frm1.Log("[판매예측]:" + ex.Message);
                frm1.Log("판매예측쿼리:" + queryString1);

            }
        }

        private void fn_get_stock_preset_stock(string orderdate1, string orderdate2, Form1 frm1)
        {

            myConnection = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("213");
            string queryString1 = "";
            string ckYYYYMM = "";
            SqlDataAdapter adapter = new SqlDataAdapter();

            ckYYYYMM = String.Format("{0:yyyyMM}", Convert.ToDateTime(orderdate1));

            // frm1.sendSms("NEWPOS_SCM  입고량 생성 시작");
            frm1.Log("NEWPOS_SCM 입고량  생성 시작");

         
            string queryString = "select  a.orderdate, right(a.storeno,4) as storeno, a.ppmidx,  SUM(ISNULL(a.amt_result,0)) as amt_pre ,  SUM(ISNULL(a.amt_input,0)) as amt_input ,(case when (sum(a.amt_result) <= 0) THEN 1 ELSE 0 END) as amt_result       "
                            + " from pos_store_stock_" + ckYYYYMM + "  as a with( nolock )  "
                             + " right join  webdb_se.dbo.stock_preset4_2019 as b with( nolock ) on a.orderdate = b.sdate and right(a.storeno,4) = b.storeno and a.ppmidx = b.ppmidx   "
                             + "where (b.sdate >=  '" + orderdate1 + "' and b.sdate <= '" + orderdate2 + "') and a.storeno <> ''  "
                           // + " from pos_store_stock  with( nolock )where orderdate >=  '" + orderdate1 + "' and orderdate <= '" + orderdate2 + "'   and a.storeno <> '' "
                           + " group by a.storeno, a.ppmidx , orderdate  "
                           // + " HAVING   sum(amt_pre + amt_input + amt_result) <> 0 "
                           + "	 order by orderdate, a.storeno, a.ppmidx   ";

            adapter.SelectCommand = new SqlCommand(queryString, myConnection);
            adapter.SelectCommand.CommandTimeout = 0;
            SqlCommand command = new SqlCommand(queryString, myConnection);


            try
            {

                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable table = ds.Tables[0];
                DataRowCollection rows = table.Rows;

                string storeno = "";
                string ppmidx = "";
                string ckorderdate = "";
                int amt_input = 0;
                int amt_pre = 0;
                int amt_result = 0;

                myConnection.Dispose();
                myConnection.Close();
                myConnection_201 = DbConnection.GetRemoteDataInstance().getRemoteDbConnection("azure");

                foreach (DataRow dr in rows)
                {
                    storeno = dr["storeno"].ToString();
                    ckorderdate = String.Format("{0:yyyy-MM-dd}", Convert.ToDateTime(dr["orderdate"].ToString()));
                    ppmidx = dr["ppmidx"].ToString();
                    amt_input = Convert.ToInt32(dr["amt_input"].ToString());
                    amt_pre = Convert.ToInt32(dr["amt_pre"].ToString());
                    amt_result = Convert.ToInt32(dr["amt_result"].ToString());


                    queryString1 = "   UPDATE scm_stock_preset SET "
                                                + "   amt_input = '" + amt_input + "'"
                                                + "   ,amt_pre = '" + amt_pre + "'"
                                                + "  ,amt_soldout = '" + amt_result + "'"
                                                + "   WHERE orderdate = '" + ckorderdate + "' and storeno = '" + storeno + "'  AND  ppmidx = '" + ppmidx + "'";

                    //Log((String.Format("[in_stock] 쿼리[{0}] : ", queryString1)));

                    SqlCommand command1 = new SqlCommand(queryString1, myConnection_201);
                    command1.CommandText = queryString1;
                    command1.ExecuteNonQuery();

                }
                frm1.sendSms("NEWPOS_SCM 입고량  생성 완료(4/4)");
                frm1.Log("NEWPOS_SCM 입고량  생성 완료");
                myConnection_201.Dispose();
                myConnection_201.Close();
            }
            catch (Exception ex)
            {
                frm1.Log("[입고량]:" + ex.Message);
                frm1.Log("입고량 쿼리:" + queryString1);
            }

        }

    }
}
