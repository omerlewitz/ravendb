using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Tools;
using StressTests.Issues;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    await using (var test = new SetupSecuredClusterUsingRvn(testOutputHelper))
                    {
                         await test.Should_Create_Secured_Cluster_From_Rvn_Using_Lets_Encrypt_Mode();
                         await test.Should_Create_Secured_Cluster_Generating_Self_Singed_Cert_And_Setup_Zip_File_From_Rvn();
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
