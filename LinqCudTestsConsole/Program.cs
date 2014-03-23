using CqlSharp;
using CqlSharp.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Thulyadb.Dynamic;

namespace LinqCudTestsConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            CqlConnectionStringBuilder cqlConnectionStringBuilder = GetCqlConnectionStringBuilder("192.168.1.33", 9042, "cassandra", "password", "dbkeyspace");
            string userId = string.Empty;

            do
            {
                Console.WriteLine("Enter the user id to increment the page visit count (Press enter key if you want to exit):");
                userId = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(userId))
                    break;

                ThulyadbContext thulyaDb = new ThulyadbContext(cqlConnectionStringBuilder.ToString());
                TLY_UserHomeStats homeStats = (from hs in thulyaDb.TLY_UserHomeStatsTable
                                               where hs.UserId == userId
                                               select hs).SingleOrDefault();
                if (homeStats == null)
                {
                    Console.WriteLine("Record already did not exist and hence inserting...:");

                    //Insert
                    homeStats = new TLY_UserHomeStats();
                    homeStats.UserId = userId;
                    homeStats.HomeProfViewsSinceLastUpdate = 1;
                    homeStats.HomeProfViewsTotal = 1;
                    thulyaDb.TLY_UserHomeStatsTable.Add(homeStats);
                }
                else
                {
                    Console.WriteLine("Record already exists and hence incrementing (updating)...:");

                    //Update
                    homeStats.HomeProfViewsSinceLastUpdate = homeStats.HomeProfViewsSinceLastUpdate + 1;
                    homeStats.HomeProfViewsTotal = homeStats.HomeProfViewsTotal + 1;

                }
                thulyaDb.SaveChanges();

                Console.WriteLine("Records after update/insert:");
                DisplayRecords(cqlConnectionStringBuilder.ToString());

                Console.WriteLine("Enter the user id string to delete (Press enter key if you want to exit)):");
                userId = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(userId))
                    break;

                TLY_UserHomeStats homeStatsToDelete = (from hs in thulyaDb.TLY_UserHomeStatsTable
                                                       where hs.UserId == userId
                                                       select hs).SingleOrDefault();

                //Delete
                if (homeStatsToDelete != null)
                {
                    thulyaDb.TLY_UserHomeStatsTable.Delete(homeStatsToDelete);
                    thulyaDb.SaveChanges();

                    Console.WriteLine("Records after deleting :" + userId);
                    DisplayRecords(cqlConnectionStringBuilder.ToString());
                }
                else
                    Console.WriteLine("Record could not be found for :" + userId);

            } while (!string.IsNullOrWhiteSpace(userId));
        }

        private static void DisplayRecords(string cqlConnectionString)
        {
            ThulyadbContext thulyaDb = new ThulyadbContext(cqlConnectionString);
            List<TLY_UserHomeStats> homeStatsList = (from hs in thulyaDb.TLY_UserHomeStatsTable
                                                     select hs).Take(10).ToList();

            if (homeStatsList != null)
            {
                foreach (TLY_UserHomeStats homeStats in homeStatsList)
                {
                    Console.WriteLine("{0} - {1} - {2}", homeStats.UserId, homeStats.HomeProfViewsSinceLastUpdate, homeStats.HomeProfViewsTotal);
                }
            }
        }

        private static CqlConnectionStringBuilder GetCqlConnectionStringBuilder(string server, int port, string userName, string password, string keyspace)
        {
            CqlConnectionStringBuilder connectionStringBuilder = new CqlConnectionStringBuilder();
            List<string> servers = new List<string>(new string[] { server });
            connectionStringBuilder.Servers = servers;
            connectionStringBuilder.Port = port;
            connectionStringBuilder.Username = userName;
            connectionStringBuilder.Password = password;
            connectionStringBuilder.Database = keyspace;
            return connectionStringBuilder;
        }
    }
}
