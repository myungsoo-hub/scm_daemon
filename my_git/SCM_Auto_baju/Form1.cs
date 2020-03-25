using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Collections;
using System.Threading;


namespace SCM_Auto_baju
{
    public partial class Form1 : Form
    {
        string ip = "";

        //System.Timers.Timer sales_timer = new System.Timers.Timer();
        // System.Windows.Forms.Timer timer = new System.Windows.Forms.Timer();
        System.Windows.Forms.Timer formstimer = new System.Windows.Forms.Timer();
        System.Windows.Forms.Timer stock_timer = new System.Windows.Forms.Timer();
        System.Windows.Forms.Timer safe_sale_timer = new System.Windows.Forms.Timer();
        System.Windows.Forms.Timer stock_auto_timer = new System.Windows.Forms.Timer();

        //  private DirectoryInfo DirInfo;
        public string InboundFolder = string.Empty;

        SqlConnection myConnection = new SqlConnection();
        MySqlConnection MYconn = new MySqlConnection();

        IPHostEntry IPHost = Dns.Resolve(Dns.GetHostName());

        public Form1()
        {
            InitializeComponent();
            this.dateTimePicker1.Value = DateTime.Today.AddDays(4);  //5일전 데이터 생성 날짜
            this.dateTimePicker2.Value = DateTime.Today.AddDays(1); //명일 12시가 아닌 금일11시에 실행하므로 +1일 추가(3일전 예측발주)

            ip = IPHost.AddressList[0].ToString();

            label16.Text = ip;

        }


        #region Log 로그남기기
        public void Log(string buf)
        {

            string path = @InboundFolder + @"Log\" + "Log_" + System.DateTime.Now.Day.ToString().PadLeft(2, '0') + ".Log";
            if (File.Exists(path) == true)
            {
                if (File.GetLastWriteTime(path).ToLongDateString().ToString() != System.DateTime.Now.ToLongDateString().ToString())
                    File.Delete(path);
            }

            FileStream Myfs = new FileStream(path, FileMode.Append, FileAccess.Write);
            StreamWriter MySW = new StreamWriter(Myfs, Encoding.GetEncoding("euc-kr"));
            MySW.WriteLine(System.DateTime.Now.ToString("HH:mm:ss") + " : " + buf);

            MySW.Close();
        }
        #endregion

        private void button1_Click(object sender, EventArgs e)
        {
            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("124");

    
            formstimer.Interval = 1000;
            formstimer.Tick += timer_sales_Tick;
            formstimer.Start();

            safe_sale_timer.Interval = 1000;
            safe_sale_timer.Tick += timer_safe_sale_Tick;
            safe_sale_timer.Start();

     

            if (MYconn.State.ToString() == "Open")
            {
                 string DB_124 = MYconn.DataSource.ToString();
                if (DB_124 != "")
                {
                    checkBox1.Checked = true;
                }

            }
            else
            {
                checkBox1.Checked = false;
            }

            MYconn.Dispose();
            MYconn.Close();
            Log((String.Format("[MySql_124]:{0}", MYconn.State.ToString())));

            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("164");

            if (MYconn.State.ToString() == "Open")
            {
                string DB_164 = MYconn.DataSource.ToString();
                if (DB_164 != "")
                {
                    checkBox2.Checked = true;
                }
            }
            else
            {
                checkBox2.Checked = false;
            }

            MYconn.Dispose();
            MYconn.Close();
            Log((String.Format("[MySql_164]:{0}", MYconn.State.ToString())));

            MYconn = DbConnection_My.MyGetRemoteDataInstance().MygetRemoteDbConnection("18");

            if (MYconn.State.ToString() == "Open")
            {
                string DB_18 = MYconn.DataSource.ToString();
                if (DB_18 != "")
                {
                    checkBox3.Checked = true;
                }
            }
            else
            {
                checkBox3.Checked = false;
            }

            MYconn.Dispose();
            MYconn.Close();
            Log((String.Format("[MySql_18]:{0}", MYconn.State.ToString())));

            if((checkBox1.Checked == true) && (checkBox2.Checked == true) && (checkBox3.Checked == true))
            {
                label2.ForeColor = System.Drawing.Color.Blue;
                label2.Text = "정상 접속 성공";
                sendSms("[2차 시뮬레이션 판매예측값] 프로그램 시작 ");
            }
            else
            {
                label2.ForeColor = System.Drawing.Color.Red;
                label2.Text = "접속 오류";
            }

        }

        private void logToolStripMenuItem_Click(object sender, EventArgs e)
        {
            System.Diagnostics.Process.Start("explorer.exe", InboundFolder + @"Log\");
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        public void sendSms(string msg) //sms 전송
        {

            //========== [usp_InsertEmTranStrForAll] STORED PROCEDURE PARAMETERS =======================
            string para_mcode = "000000000";       //조합원코드
            string para_ocode = "A049";               //조합원소속단체코드
            string para_ccode = "9040";              //조합원소속센터코드
            string para_jcode = "9040";              //조합원소속조합코드
            string para_postcode = "CL01";          //조합원소속부서코드                  
            string para_trangubun = "Z";          //전송구분
            string para_callback = "0314283000";    //보낸번호
            string para_mpstcode = "000000000";       //보낸사람조합원코드
            string para_codestr = msg;               //전송메시지
            string para_mtel = "";                  //수신번호

            string strcon = "server=211.234.118.163;database=smsdata;uid=sms_usp;pwd=smspass12;";
            MySqlConnection smsconn = new MySqlConnection(strcon);
            smsconn.Open();

            ArrayList receiveMemberList = new ArrayList();
            receiveMemberList.Add("010-9608-1318");//명수
           // receiveMemberList.Add("010-9138-1692");//팀장님
            // receiveMemberList.Add("010-9091-5004"); //주희씨
            //receiveMemberList.Add("010-9213-0727"); //광훈씨//


            try
            {
                foreach (string item in receiveMemberList)
                {
                    para_mtel = item;

                    MySqlCommand mycommand = new MySqlCommand("usp_InsertEmTranStrForAll", smsconn);
                    mycommand.Connection = smsconn;
                    mycommand.CommandType = CommandType.StoredProcedure;

                    mycommand.Parameters.Clear();
                    mycommand.Parameters.Add(new MySqlParameter("@para_mcode", para_mcode));
                    mycommand.Parameters.Add(new MySqlParameter("@para_ocode", para_ocode));
                    mycommand.Parameters.Add(new MySqlParameter("@para_ccode", para_ccode));
                    mycommand.Parameters.Add(new MySqlParameter("@para_jcode", para_jcode));
                    mycommand.Parameters.Add(new MySqlParameter("@para_postcode", para_postcode));
                    mycommand.Parameters.Add(new MySqlParameter("@para_mtel", para_mtel));
                    mycommand.Parameters.Add(new MySqlParameter("@para_trangubun", para_trangubun));
                    mycommand.Parameters.Add(new MySqlParameter("@para_callback", para_callback));
                    mycommand.Parameters.Add(new MySqlParameter("@para_mpstcode", para_mpstcode));
                    mycommand.Parameters.Add(new MySqlParameter("@para_codestr", para_codestr));
                    mycommand.ExecuteNonQuery();
                    mycommand.Dispose();
                    mycommand = null;
                }

            }
            catch (Exception ex)
            {
                Log((String.Format("SMS 전송 오류", ex.Message)));

                if (smsconn != null)
                {
                    smsconn.Dispose();
                    smsconn = null;
                }

                Environment.Exit(0);
            }
            finally
            {
                if (smsconn != null)
                {
                    smsconn.Dispose();
                    smsconn = null;
                }
            }
        }


        //delegate void progvarcall(int var);

        //private void forecast_v3()
        //{

        //    string day_orderdate = string.Format(DateTime.Today.AddDays(1).ToString("yyyy-MM-dd")); //금일(+1)일자
        //    string orderdate = string.Format(DateTime.Today.AddDays(4).ToString("yyyy-MM-dd")); //공급일자

        //    Sales_forecast forecast = new Sales_forecast();
        //    forecast.fn_forecast_V3_D3(orderdate, day_orderdate, this); //3일전 판매예측값 생성
        //    progressBar1.PerformStep();
        //    forecast.fn_preset_V3_D3_group(orderdate, day_orderdate, this);  //3일전 [그룹단위 판매예측값] 생성
        //    progressBar1.PerformStep();
        //}

        private void ProgValueSetting(int var)
        {
            progressBar1.Value = var;
        }

        #region 11시 판매예측값 생성
        private void button2_Click(object sender, EventArgs e)
        {

            string orderdate = this.dateTimePicker1.Value.ToString("yyyy-MM-dd");  // +5일후 날짜
            string day_orderdate = this.dateTimePicker2.Value.ToString("yyyy-MM-dd"); // 금일날짜

            Sales_forecast forecast = new Sales_forecast();
            forecast.fn_forecast_V3_D3(orderdate, day_orderdate, this); //3일전 판매예측값 생성
            forecast.fn_preset_V3_D3_group(orderdate, day_orderdate, this);  //3일전 [그룹단위 판매예측값] 생성

            MessageBox.Show("11시 판매예측값 생성 완료하였습니다.");
        }


        void timer_sales_Tick(object sender, EventArgs e)
        {
            label5.Text = DateTime.Now.ToString("HH:mm:ss");
            string DD = string.Format(DateTime.Now.ToString("dd"));

            //11시 판매예측값 생성
            if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 225955) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 230000)))  //11시 판매예측값 생성
          //if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >=135355) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 135400)))  //11시 판매예측값 생성
            {
                while (true)
                {

                    if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 230001)
                     // if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 135401)
                    {

                        string ppm_orderdate = string.Format(DateTime.Today.AddDays(0).ToString("yyyy-MM-dd")); //Newpos_SCM 정가판매 생성 일자(금일)

                        string day_orderdate = string.Format(DateTime.Today.AddDays(1).ToString("yyyy-MM-dd")); //금일(+1)일자
                        string orderdate_4 = string.Format(DateTime.Today.AddDays(4).ToString("yyyy-MM-dd")); //공급일자

                        /*
                        //----안전재고, 정가판매 수량 날짜계산
                        //DateTime today = DateTime.Now.Date; //현재 날짜 확인
                        //var ex_today = today.AddMonths(0); // 월 구함
                        //var ex_sdate = ex_today.AddDays(1 - today.Day); //현재 날짜 기준의 월의 날짜 1일 구함

                        //string safe_sale_today = string.Format(DateTime.Today.AddDays(1).ToString("dd")); // 자료 생성일이 01일  확인
                        //string safe_sale_sdate = string.Format(ex_sdate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경
                        //string stock_YYYYMM = string.Format(DateTime.Today.AddDays(0).ToString("yyyyMM"));  //현월 테이블 확인
                        //                                                                                    //string stock_YYYYMM = "YYYYMM";  //재고 월테이블 날짜 확인
                        //                                                                                    //   string safe_sale_edate = string.Format(DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")); //전월 마지막 날짜 구함
                        //string safe_junnga = this.jungga_text.Text;
                        //string type = "A";  //A:자동시작, B:수동시작

                        //string YYYY = string.Format(DateTime.Today.AddDays(0).ToString("yyyy"));
                        //string MM = string.Format(DateTime.Today.AddDays(0).ToString("MM"));

                        //int totlaDay = DateTime.DaysInMonth(Convert.ToInt32(YYYY), Convert.ToInt32(MM)); //해당 년월에 일수 계산
                        //----안전재고, 정가판매 수량 날짜계산
                        */

                        sendSms("[" + DateTime.Now.AddDays(1).ToString("yyyy-MM-dd") + "][2차 시뮬레이션 판매예측값] 생성시작(" + String.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Now.AddDays(0).ToString()) + ")");

                        /*
                        //매월 4일 기준 안전재고, 판매예측값 생성
                        //if (safe_sale_today.Equals("01"))
                        //{
                        //    Log("[" + safe_sale_sdate + "~" + ppm_orderdate + "] 안전재고,정가판매 통합 자료 생성");
                        //    safe_sale_total safe_sale = new safe_sale_total();
                        //    safe_sale.fn_safe_sale(safe_sale_sdate, ppm_orderdate, safe_junnga, stock_YYYYMM, type, totlaDay, this); //안전재고량 , 정가판매량 구함 (금일기준 -30일)
                        //}
                        //매월 4일 기준 안전재고, 판매예측값 생성
                        */

                        //---------------------------------------
                        //ppm 정가판매 자료 생성

                        sale_ppm_daily ppm_daily = new sale_ppm_daily();
                        Log("[" + ppm_orderdate + "] Newpos_ppm 기초 자료 생성");
                        ppm_daily.fn_ppmdaily(ppm_orderdate, ppm_orderdate, this); //Newpos_SCM 정가판매 생성

                        //ppm 정가판매 자료 생성
                        //---------------------------------------
                        //판매 예측값자료 생성
                        if (!(orderdate_4.Equals("2019-09-13")) && !(orderdate_4.Equals("2019-09-14")))
                        {
                            Sales_forecast forecast = new Sales_forecast();
                            Log("[" + orderdate_4 + "] D-4 판매예측값 생성시작");
                            forecast.fn_forecast_V3_D3(orderdate_4, day_orderdate, this); //3일전 판매예측값 생성
                            forecast.fn_preset_V3_D3_group(orderdate_4, day_orderdate, this);  //3일전 [그룹단위 판매예측값] 생성
                        }

                        //판매 예측값자료 생성
                        //---------------------------------------

                        this.dateTimePicker2.Value = DateTime.Today.AddDays(2); //금일(+1)일자 그 다음날
                        this.dateTimePicker1.Value = DateTime.Today.AddDays(5); //공급일자 그 다음날

                        this.dateTimePicker5.Value = DateTime.Today.AddDays(1); //금일(+1)일자 그 다음날
                        this.dateTimePicker6.Value = DateTime.Today.AddDays(1); //공급일자 그 다음날


                        break;
                    }
                    else
                    {
                        label5.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
            }
            //매장 자동 입고    
            if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 045955) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 050000)))  //매장 자동 입고                                                            
            {
                while (true)
                {
                    if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 050001)
                    {
                        string orderdate_D = string.Format(DateTime.Now.ToString("yyyy-MM-dd"));
                        string type = "A";

                        stock_auto_input sock_auto = new stock_auto_input();
                        sock_auto.fn_stockAuto(orderdate_D, type, ip, this);

                        this.dateTimePicker9.Value = DateTime.Today.AddMonths(0); //금일일자 그 다음달

                        break;
                    }
                    else
                    {
                        label5.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
            }
            //매월 24일 비인기 품목
            if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 165955) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 170000)))  //24일 비인기 품목                                                             
            {
                while (true)
                {
                    if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 170001)
                    //       if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 105101)
                    {
                        if (DD.Equals("24"))
                        {
                            store_product_unpopular unpopular = new store_product_unpopular();
                            unpopular.fn_unpopular_list(string.Format(DateTime.Now.ToString("yyyyMM")), this);
                            break;
                        }
                        break;
                    }
                    else
                    {
                        label5.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
            }
            //05시 SCM 입고예측값 생성
            if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 061055) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 061100))) 
            {
                while (true)
                {
                    if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 061101)
                    {
                        string orderdate_M = string.Format(DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")); //공급일자

                        scm_stock_preset preset = new scm_stock_preset();

                        Log("[" + orderdate_M + "] Newpos_SCM 예측 자료 생성");

                        preset.fn_scmstock(orderdate_M, orderdate_M, this); //Newpos_SCM 예측값 생성

                        this.dateTimePicker3.Value = DateTime.Today.AddDays(0); //금일(+1)일자 그 다음날
                        this.dateTimePicker4.Value = DateTime.Today.AddDays(0); //공급일자 그 다음날

                        break;
                    }
                    else
                    {
                        label5.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
            }         
            //년,월 표준편차 생성
            if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 065955) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 070000)))  //년,월 표준편차 생성                                                  
            {
                //매년 01월01일에는 년간(구제품)
                //매년 01월02에는 월간(신제품)이 실행되어야됨 
                //매월 01일 신제품기준을 생성
                while (true)
                {
                    if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 070001)
                    {
                        safe_standard_deviation standard_deviation = new safe_standard_deviation();
                        string gubun = "";
                        string except = this.except_text.Text;
                        string orderdate = string.Format(DateTime.Today.AddDays(0).ToString("MM-dd")); //현재 날짜 확인(금일) 01-01
                        string day_orderdate = string.Format(DateTime.Today.AddDays(0).ToString("dd")); //현재 날짜 확인(금일) 01-01

                        if (orderdate == "01-01") //연간 한번만 실행
                        {
                            DateTime today = DateTime.Now.Date; //현재 날짜 확인
                            var s_today = today.AddMonths(-12); // 전월 구함
                            var ex_sdate = s_today.AddDays(1 - today.Day); //전월의 날짜 1일 구함

                            var ex_etoday = today.AddMonths(0); // 당월 구함
                            var ex_edate = ex_etoday.AddDays(-1);//금일에 -1일

                            string sdate = string.Format(ex_sdate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경
                            string edate = string.Format(ex_edate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경

                            gubun = "Y";
                            standard_deviation.fn_safe_standard_deviation(sdate, edate, except, gubun, this);

                        }
                        else if (orderdate == "01-02") //01월02일에 월간 실행
                        {
                            /////////////////////////////////////////////////////월간///////////////////////////////////////////////
                            DateTime today_2 = DateTime.Now.Date; //현재 날짜 확인
                            var ex_today_2 = today_2.AddMonths(-1); // 전월 구함
                            var ex_sdate_2 = ex_today_2.AddDays(1 - today_2.Day); //전월의 날짜 1일 구함

                            var ex_etoday_2 = today_2.AddMonths(0); // 당월 구함
                            var ex_edate_2 = ex_etoday_2.AddDays(-2);//금일에 -1일
                            string sdate_2 = string.Format(ex_sdate_2.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경
                            string edate_2 = string.Format(ex_edate_2.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경

                            gubun = "M";

                            standard_deviation.fn_safe_standard_deviation(sdate_2, edate_2, except, gubun, this);
                        }
                        else if( day_orderdate == "01")
                        {
                          
                            DateTime today = DateTime.Now.Date; //현재 날짜 확인
                            var ex_today = today.AddMonths(-1); // 전월 구함
                            var ex_sdate = ex_today.AddDays(1 - today.Day); //전월의 날짜 1일 구함

                            var ex_etoday = today.AddMonths(0); // 당월 구함
                            var ex_edate = ex_etoday.AddDays(-1);//금일에 -1일
                            string sdate = string.Format(ex_sdate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경
                            string edate = string.Format(ex_edate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경

                            gubun = "M";

                            standard_deviation.fn_safe_standard_deviation(sdate, edate, except, gubun, this);
                            
                        }


                        break;
                    }
                    else
                    {
                        label5.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
            }
            
        }

        #endregion

        //private void Calculate(int i)
        //{
        //    double pow = Math.Pow(i, i);
        //}

        #region newpos 입고데이터 생성

        private void button3_Click(object sender, EventArgs e)
        {
            string orderdate1 = this.dateTimePicker3.Value.ToString("yyyy-MM-dd");
            string orderdate2 = this.dateTimePicker4.Value.ToString("yyyy-MM-dd");

            //progressBar1.Maximum = 100;
            //progressBar1.Step = 1;
            //progressBar1.Value = 0;
            //backgroundWorker1.RunWorkerAsync();

            scm_stock_preset scmstock = new scm_stock_preset();
            scmstock.fn_scmstock(orderdate1, orderdate2, this); //scm 자료 생성

            MessageBox.Show(" newpos 입고데이터 생성 완료하였습니다.");

        }
        #endregion


        #region ppm 정가판매량 생성
        private void button4_Click(object sender, EventArgs e)
        {
            string orderdate5 = this.dateTimePicker5.Value.ToString("yyyy-MM-dd");
            string orderdate6 = this.dateTimePicker6.Value.ToString("yyyy-MM-dd");

            sale_ppm_daily ppmdaily = new sale_ppm_daily();
            ppmdaily.fn_ppmdaily(orderdate5, orderdate6, this); //scm 자료 생성

            MessageBox.Show(" ppm 정가판매량 생성 완료하였습니다.");
        }
        #endregion


        #region 안전재고,정가판매 수량 제외 생성
        private void timer_safe_sale_Tick(object sender, EventArgs e)
        {
            label5.Text = DateTime.Now.ToString("HH:mm:ss");

             if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 224455) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 224500)))  //제외할 22시45분 안전재고,정가판매 수량  생성
           // if (((Int32.Parse(DateTime.Now.ToString("HHmmss")) >= 101155) && (Int32.Parse(DateTime.Now.ToString("HHmmss")) <= 101200)))  //제외할 22시45분 안전재고,정가판매 수량  생성
            {
                while (true)
                {
                    if (Int32.Parse(DateTime.Now.ToString("HHmmss")) > 224501)
                    {

                        //----안전재고, 정가판매 수량 날짜계산
                        string ppm_orderdate = string.Format(DateTime.Today.AddDays(0).ToString("yyyy-MM-dd")); //Newpos_SCM 정가판매 생성 일자(금일)

                        DateTime today = DateTime.Now.Date; //현재 날짜 확인
                        var ex_today = today.AddMonths(0); // 월 구함
                        var ex_sdate = ex_today.AddDays(1 - today.Day); //현재 날짜 기준의 월의 날짜 1일 구함

                        string safe_sale_today = string.Format(DateTime.Today.AddDays(1).ToString("dd")); // 자료 생성일이 01일  확인
                        string safe_sale_sdate = string.Format(ex_sdate.AddDays(0).ToString("yyyy-MM-dd"));  //전월 날짜 1일을 YYYY-MM-DD형식으로 변경
                        string stock_YYYYMM = string.Format(DateTime.Today.AddDays(0).ToString("yyyyMM"));  //현월 테이블 확인
                                                                                                            //string stock_YYYYMM = "YYYYMM";  //재고 월테이블 날짜 확인
                                                                                                            //   string safe_sale_edate = string.Format(DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")); //전월 마지막 날짜 구함
                        string safe_junnga = this.jungga_text.Text;
                        string type = "A";  //A:자동시작, B:수동시작

                        string YYYY = string.Format(DateTime.Today.AddDays(0).ToString("yyyy"));
                        string MM = string.Format(DateTime.Today.AddDays(0).ToString("MM"));

                        int totlaDay = DateTime.DaysInMonth(Convert.ToInt32(YYYY), Convert.ToInt32(MM)); //해당 년월에 일수 계산
                        //----안전재고, 정가판매 수량 날짜계산

                        if (safe_sale_today.Equals("01"))
                        {
                            Log("[" + safe_sale_sdate + "~" + ppm_orderdate + "] 안전재고,정가판매 통합 자료 생성");
                            safe_sale_total safe_sale = new safe_sale_total();
                            safe_sale.fn_safe_sale(safe_sale_sdate, ppm_orderdate, safe_junnga, stock_YYYYMM, type, totlaDay, this); //안전재고량 , 정가판매량 구함 (금일기준 -30일)

                            //store_stock_abc_accrue abc_accrue = new store_stock_abc_accrue();
                            //abc_accrue.fn_store_stock_abc_accrue(safe_sale_sdate, ppm_orderdate, this); //ABC 등급설정

                        }

                        this.dateTimePicker7.Value = ex_sdate.AddMonths(1); //금일일자 그 다음달
                        this.dateTimePicker8.Value = ex_sdate.AddMonths(1); //공급일자 그 다음달

                        break;
                    }
                    else
                    {
                        label5.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
            }
        }
        #endregion

        private void button5_Click(object sender, EventArgs e)
        {
            string safe_junnga = this.jungga_text.Text;
            string orderdate7 = this.dateTimePicker7.Value.ToString("yyyy-MM-dd");
            string orderdate8 = this.dateTimePicker8.Value.ToString("yyyy-MM-dd");

            string stock_edate = this.dateTimePicker7.Value.ToString("yyyyMM"); //재고 월별 테이블 확인
            string type = "B";

            string YYYY = this.dateTimePicker7.Value.ToString("yyyy");
            string MM = this.dateTimePicker7.Value.ToString("MM");

            int totlaDay = DateTime.DaysInMonth(Convert.ToInt32(YYYY), Convert.ToInt32(MM)); //해당 년월에 일수 계산

            safe_sale_total safe_sale = new safe_sale_total();
            safe_sale.fn_safe_sale(orderdate7, orderdate8, safe_junnga, stock_edate, type, totlaDay, this); //매월 생성일 기준 1일 안전재고량(<3), 정가판매량(<7) 계산

        }
       

        private void button6_Click(object sender, EventArgs e) //수동 입고
        {

            string orderdate = this.dateTimePicker9.Value.ToString("yyyy-MM-dd");
            string type = "B";

            if (Convert.ToInt32(string.Format(DateTime.Now.ToString("HH"))) >= 00 && Convert.ToInt32(string.Format(DateTime.Now.ToString("HH"))) < 05)
            {
                MessageBox.Show("00 ~ 05시까지는 입고실행을 제한합니다.");
                return;
            }
            else
            {
                stock_auto_input sock_auto = new stock_auto_input();
                sock_auto.fn_stockAuto(orderdate, type, ip, this);
            }
        }
        
        private void button7_Click(object sender, EventArgs e)   //수동 비인기물품생성
        {
            this.orderMonth.Text = string.Format(DateTime.Now.ToString("yyyyMM"));  //금일자

            store_product_unpopular unpopular = new store_product_unpopular();
            unpopular.fn_unpopular_list(orderMonth.Text,this);

            MessageBox.Show("비인기물품생성 완료하였습니다.");

        }

        private void button8_Click(object sender, EventArgs e)
        {
            
            string orderdate7 = this.dateTimePicker10.Value.ToString("yyyy-MM-dd");
            string orderdate8 = this.dateTimePicker11.Value.ToString("yyyy-MM-dd");
            string except = this.except_text.Text;
            string gubun = "Y";

            safe_standard_deviation standard_deviation = new safe_standard_deviation();
            standard_deviation.fn_safe_standard_deviation(orderdate7, orderdate8, except, gubun, this);
            //standard_deviation.fn_safe_standard_deviation("2018-01-01", "2018-12-31", except, gubun, this);


            MessageBox.Show("연간 표준편차 완료하였습니다.");
        }

        private void button9_Click(object sender, EventArgs e)
        {
            string orderdate7 = this.dateTimePicker10.Value.ToString("yyyy-MM-dd");
            string orderdate8 = this.dateTimePicker11.Value.ToString("yyyy-MM-dd");
            string except = this.except_text.Text;

            string gubun = "M";
            safe_standard_deviation standard_deviation = new safe_standard_deviation();

            standard_deviation.fn_safe_standard_deviation(orderdate7, orderdate8, except, gubun, this);
            //standard_deviation.fn_safe_standard_deviation("2018-01-01", "2018-12-31", except, gubun, this);

            MessageBox.Show("월간 표준편차 완료하였습니다.");

        }

        private void button10_Click(object sender, EventArgs e)
        {
            string orderdate9 = this.dateTimePicker12.Value.ToString("yyyy-MM-dd");
            string orderdate10 = this.dateTimePicker13.Value.ToString("yyyy-MM-dd");

            store_stock_abc_accrue stock_abc_accrue = new store_stock_abc_accrue();
            stock_abc_accrue.fn_store_stock_abc_accrue(orderdate9, orderdate10, this);
            //stock_abc_accrue.fn_store_stock_abc_accrue("2018-01-01", "2018-12-31",  this);

            MessageBox.Show("월간 ABC누적등급 완료하였습니다.");
        }





        //private void backgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        //{
        //    var backgroundWorker = sender as BackgroundWorker;
        //    for (int j = 0; j < 100000; j++)
        //    {
        //        Calculate(j);
        //        backgroundWorker.ReportProgress((j * 100) / 100000);
        //    }
        //}

        //private void backgroundWorker_ProgressChanged(object sender, ProgressChangedEventArgs e)
        //{
        //    progressBar1.Value = e.ProgressPercentage;
        //}



    }
}
