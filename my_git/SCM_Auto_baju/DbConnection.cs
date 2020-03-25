using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.Data;
using System.IO;


namespace SCM_Auto_baju
{
    class DbConnection
    {
       
      
        SqlConnection myConnection = new SqlConnection();


        private static DbConnection remoteInstance;
        private readonly SqlConnection remoteConn = new SqlConnection();

        public string InboundFolder = string.Empty;



        public static DbConnection GetRemoteDataInstance()
        {
            if (remoteInstance == null || string.IsNullOrEmpty(remoteInstance.remoteConn.Database))
            {
                remoteInstance = new DbConnection();
            }
            return remoteInstance;
        }

        public SqlConnection getRemoteDbConnection(string str)
        {

            try
            {
                if (remoteConn.State != System.Data.ConnectionState.Open)
                {
                    remoteConn.ConnectionString = remoteInstance.GetConnectionString(str);
                    remoteConn.Open();
                    //Log(remoteConn.State.ToString());
                    Log((String.Format("[MsSql]:{0}", remoteConn.State.ToString())));
                }

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
                }

                Log(errorMessages.ToString());
 
            }
            return remoteConn;

        }


        private string GetConnectionString(String str) //MSsql연결
        {
            // To avoid storing the connection string in your code, 
            // you can retrieve it from a configuration file, using the 
            // System.Configuration.ConfigurationSettings.AppSettings property 
          /*
             //테스트
             if (str == "213")
             {
                      //테스트
                         return "Server=192.168.1.201;Database=WEBDB;Uid=test_webdb; Pwd=test_webdb_;";
                   //   return "Server=211.43.220.213;Database=WEBDB;Uid=user_kms; Pwd=pass_kms_21143220213_;";
                  }
             else if (str == "azure")
             {
                      //return "Server=211.43.220.213;Database=WEBDB;Uid=user_kms; Pwd=pass_kms_21143220213_;";
                       return "Server=posscm.database.windows.net;Database=scmdb;Uid=posteam; Pwd=znqtodguqwjstks!@)(3;";
                     // return "Server=192.168.1.201;Database=WEBDB;Uid=test_webdb; Pwd=test_webdb_;";
                  }
             else if (str == "master")
             {
                 return "";
             }
             else
             {
                 return "";
             }
*/
              //실서버
              if (str == "213")
              {
              //실서버
                return "Server=211.43.220.213;Database=WEBDB;Uid=user_kms; Pwd=pass_kms_21143220213_;";  
              }
               else if (str == "azure")
              {
              //return "Server=211.43.220.213;Database=WEBDB;Uid=user_kms; Pwd=pass_kms_21143220213_;";
                  return "Server=posscm.database.windows.net;Database=scmdb;Uid=posteam; Pwd=znqtodguqwjstks!@)(3;";
              // return "Server=192.168.1.201;Database=WEBDB;Uid=test_webdb; Pwd=test_webdb_;";
              }
                  else if (str == "master")
              {
                  return "";
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
