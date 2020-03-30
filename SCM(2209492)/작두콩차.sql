SELECT t1.sdate as sdate,  t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201806 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx
 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-06-01' and a.orderdate <='2018-06-31')
   UNION ALL 
   SELECT t1.sdate as sdate, t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201807 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx
 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-07-01' and a.orderdate <='2018-07-31')
    UNION ALL 
   SELECT t1.sdate as sdate,  t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201808 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx

 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-08-01' and a.orderdate <='2018-08-31')
    UNION ALL 
   SELECT t1.sdate as sdate,  t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201809 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx

 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-09-01' and a.orderdate <='2018-09-31')
    UNION ALL 
   SELECT t1.sdate as sdate,  t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201810 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx

 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-10-01' and a.orderdate <='2018-10-31')
    UNION ALL 
   SELECT t1.sdate as sdate,  t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201811 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx

 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-11-01' and a.orderdate <='2018-11-31')
    UNION ALL 
   SELECT t1.sdate as sdate,  t3.bar_code, amt_input, amt_sale , isnull(qty_total - qty_discount,0) as amt_sale_jungga,  amt_result,
t1.scm_gb, t1.stock_only_ab, t1.reg_gb,  t1.scm_susik, t1.amt_bundle, t1.amt_balju_pre, t1.amt_balju,
 t1.amt_sale_d3, t1.amt_sale_d2, t1.amt_sale_d1, t1.amt_sale_d,
  t1.amt_pda_d3, t1.amt_pda_d2, t1.amt_pda_d1, t1.amt_safe_d,
   t1.amt_magam, t1.amt_input_d3, t1.amt_input_d2, t1.amt_input_d1, 
   t1.amt_adjust1, t1.amt_adjust2, t1.amt_adjust3, t1.amt_adjust4, t1.amt_adjust5, t1.amt_adjust6, t1.amt_adjust7
FROM 
WEBDB_SE.dbo.stock_preset4_2018 as t1 with (nolock)
LEFT OUTER JOIN pos_product_master as t3 with (nolock) ON t1.ppmidx = t3.idx 
LEFT OUTER JOIN pos_store_stock_201812 as a with (nolock) ON t3.idx  = a.ppmidx  and t1.sdate = a.orderdate and t1.storeno = right(a.storeno,4)
LEFT OUTER JOIN pos_sale_a01_ppm_daily as s2 with (nolock) ON t1.sdate = s2.orderdate and a.storeno = s2.storeno and a.ppmidx = s2.ppmidx

 WHERE  t1.vc = '1' AND  icoop_code ='56101K9600' and t1.storeno ='6085' and (a.orderdate >='2018-12-01' and a.orderdate <='2018-12-31')
 order by sdate

