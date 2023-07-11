using System;
using MySql.Data.MySqlClient;
using System.Data;
using System.IO;
using Newtonsoft.Json.Linq;

namespace sql_batch_insert
{
    class Program
    {
        static void Main(string[] args)
        {
            string ConnectionString = "Server=127.0.0.1;Database=deneme;User Id=root;Password=12345";
            string firstName, lastName, address, city;

            using (MySqlConnection mConnection = new MySqlConnection(ConnectionString))
            {
                mConnection.Open();

                string jsonContent = File.ReadAllText("../../../file.json");
                //alınan .json dosyası parse'lanıp jsonArray'e aktarılıyor
                JArray jsonArray = JArray.Parse(jsonContent);
                DataTable temporaryTable = new DataTable();

                temporaryTable.Columns.Add("FirstName");
                temporaryTable.Columns.Add("LastName");
                temporaryTable.Columns.Add("Address");
                temporaryTable.Columns.Add("City");
                //array'daki her sey gecici tabloya aktariliyor
                foreach (JObject jsonObject in jsonArray)
                {
                    lastName = jsonObject["lastname"].ToString();
                    firstName = jsonObject["firstname"].ToString();
                    address = jsonObject["address"].ToString();
                    city = jsonObject["city"].ToString();

                    temporaryTable.Rows.Add(firstName, lastName, address, city);
                }

                using (MySqlTransaction tran = mConnection.BeginTransaction(System.Data.IsolationLevel.Serializable))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = mConnection;
                        cmd.Transaction = tran;
                        cmd.CommandText = "SELECT * FROM denemeTable";
                        using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                        {
                            da.UpdateBatchSize = 1000;
                            using (MySqlCommandBuilder cb = new MySqlCommandBuilder(da))
                            {
                                //gecici tablo sql serverdaki tabloya aktarılıyor
                                da.Update(temporaryTable);
                                tran.Commit();
                            }
                        }
                    }
                }
            }
        }
    }
}

