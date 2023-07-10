using System;
using MySql.Data.MySqlClient;
using System.Data;
using Newtonsoft.Json;
using System.IO;


//Newtonsoft.Json (13.0.3) ve MySql.Data (8.0.33) NuGet'lerini kullandım.


namespace sql_batch_insert
{
    class Program
    {
        static void Main(string[] args)
        {
            string ConnectionString = "Server=127.0.0.1;Database=deneme;User Id=root;Password=12345";
            string Command = "INSERT INTO Persons (FirstName, LastName, Address, City ) VALUES (@FirstName, @LastName, @Address, @City);";

            //mysql server ile baglanti yapiliyor
            using (MySqlConnection mConnection = new MySqlConnection(ConnectionString))
            {
                try
                {
                    mConnection.Open();
                    //json dosyası acilip okunup string olarak json degiskenine aktariliyor
                    StreamReader r = new StreamReader("../../../file.json");
                    string json = r.ReadToEnd();
                    //newtonsoft.json kutuphanesiyle json formundaki string slice'lanıp array'e atiliyor
                    dynamic array = JsonConvert.DeserializeObject(json);

                    //array'daki her sey tabloya aktariliyor
                    foreach (var item in array)
                    {
                        using (MySqlCommand myCmd = new MySqlCommand(Command, mConnection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.Parameters.AddWithValue("@FirstName", item.firstname);
                            myCmd.Parameters.AddWithValue("@LastName", item.lastname);
                            myCmd.Parameters.AddWithValue("@Address", item.address);
                            myCmd.Parameters.AddWithValue("@City", item.city);
                            myCmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception err)
                {
                    Console.WriteLine(err.Message);
                }
            }
        }
    }
}