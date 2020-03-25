using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Windows.Forms;

namespace SCM_Auto_baju
{
    static class Program
    {
        /// <summary>
        /// 해당 응용 프로그램의 주 진입점입니다.
        /// </summary>
        [STAThread]
        static void Main()
        {
            try
            {
                int cnt = 0;

                Process[] procs = Process.GetProcesses();

                foreach (Process p in procs)
                {
                    if (p.ProcessName.Equals("SCM_Auto_baju") == true)
                        cnt++;
                }

                if (cnt > 1)
                {
                    MessageBox.Show("이미 실행중입니다.");
                    return;
                }
                else
                {
                    Application.EnableVisualStyles();
                    Application.SetCompatibleTextRenderingDefault(false);
                    Application.Run(new Form1());
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Program - Error");
            }

        }
    }
    
}
