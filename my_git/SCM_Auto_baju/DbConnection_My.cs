using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;
using System.IO;

namespace SCM_Auto_baju
{
    class DbConnection_My
    {
        MySqlConnection MYconn = new MySqlConnection();

        private static DbConnection_My remoteInstance;
        private readonly MySqlConnection remoteConn = new MySqlConnection();

        public string InboundFolder = string.Empty;


        public static DbConnection_My MyGetRemoteDataInstance()
        {
            if (remoteInstance == null || string.IsNullOrEmpty(remoteInstance.remoteConn.Database))
            {
                remoteInstance = new DbConnection_My();
            }
            return remoteInstance;
        }

        public MySqlConnection MygetRemoteDbConnection(string str)
        {

            try
            {
                if (remoteConn.State != System.Data.ConnectionState.Open)
                {
                    remoteConn.ConnectionString = remoteInstance.MyGetConnectionString(str);
                    remoteConn.Open();
                   // Log(remoteConn.State.ToString());
                    //Log((String.Format("[MySql]:{0}", remoteConn.State.ToString())));
                }

            }
            catch (MySqlException e)
            {

                Log(e.ToString());
            }
            return remoteConn;

        }


        private string MyGetConnectionString(String str) //Mysql연결
        {
            // To avoid storing the connection string in your code, 
            // you can retrieve it from a configuration file, using the 
            // System.Configuration.ConfigurationSettings.AppSettings property 
       /*
                           //테스트
                           if (str == "164")
                           {
                               //테스트
                               return "server=192.168.1.1;database=coopbase;uid=pos;password=pass_pos;CharSet=utf8;";
                           }
                           else if (str =="18")
                           {
                               //테스트
                               return "server=192.168.1.1;database=coopbase;uid=pos;password=pass_pos;CharSet=utf8;";
                           }
                           else if (str == "28")      //CUPG 접속
                           {
                               //테스트
                               return "server=192.168.1.16;database=cupgdb;uid=us_pos_id;password=pos12#$;CharSet=utf8;";

                           }
                           else if (str == "124")      //웹
                           {
                               //테스트
                               //return "server=192.168.1.15;database=naturaldream;uid=test_2team;password=0528!;CharSet=utf8;";
                               return "server=icooptest.cjfanoqpilmz.ap-northeast-2.rds.amazonaws.com;database=naturaldream;uid=erp_natural_test;password=testvhtm63;CharSet=utf8;";

                           }
                           else if (str == "1")
                           {
                               //테스트
                               return "server=192.168.1.1;database=coopbase;uid=pos;password=pass_pos;CharSet=utf8;";
                           }
                           else
                           {
                               return "";
                           }
*/
            //실서버
                     if (str == "164")
                     {
                     //실서버
                     return "server=211.234.118.164;database=coopbase;uid=kowm;pwd=auction;CharSet=utf8;"; 
                     }
                     else if (str == "18")
                     {
                     //실서버
                     return "server=211.234.123.18;database=coopbase;uid=kowm;pwd=auction;CharSet=utf8;"; 
                     }
                     else if (str == "28")      //CUPG 접속
                     {
                     //실서버
                     return "server=211.234.123.28;database=cupgdb;uid=kowm;password=auction;CharSet=utf8;";

                     }
                     else if (str == "124")      //웹
                     {
                     //실서버
                     //return "server=211.234.123.124;database=naturaldream;uid=kowm;password=auction;CharSet=utf8;";
                     return "server=icoop.cjfanoqpilmz.ap-northeast-2.rds.amazonaws.com;database=naturaldream;uid=kowm;password=auction;CharSet=utf8;";

                     }
                     else if (str == "1")
                     {
                     //테스트
                     return "server=192.168.1.1;database=coopbase;uid=pos;password=pass_pos;CharSet=utf8;";
                     }
                    else if (str == "42")
                    {
                        //테스트
                        return "server=192.168.1.42;database=maria;uid=user_hooni;password=vhtmxla!1;CharSet=utf8;PORT=14335;";
                    }
                    else
                     {
                     return "";
                     }

        }

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

    }
}
