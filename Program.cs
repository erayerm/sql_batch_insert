using System;
using MySql.Data.MySqlClient;
using System.Data;
using Newtonsoft.Json;
using System.IO;
using System.Linq;
using System.Collections.Generic;
//Newtonsoft.Json (13.0.3) ve MySql.Data (8.0.33) NuGet'lerini kullandım.


namespace sql_batch_insert
{
    class Program
    {
        static void Main(string[] args)
        {
            string ConnectionString = "Server=127.0.0.1;Database=deneme;User Id=root;Password=12345";
            //string Command = "INSERT INTO Persons (FirstName, LastName, Address, City ) VALUES (@FirstName, @LastName, @Address, @City);";
            string firstName, lastName, address, city;
            int counter = 0;

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

                    DataTable temporaryTable = new DataTable();
                    temporaryTable.Columns.Add("FirstName");
                    temporaryTable.Columns.Add("LastName");
                    temporaryTable.Columns.Add("Address");
                    temporaryTable.Columns.Add("City");
                    
                    //array'daki her sey tabloya aktariliyor
                    foreach (var item in array)
                    {
                        firstName = item.firstname;
                        lastName = item.lastname;
                        address = item.address;
                        city = item.city;

                        temporaryTable.Rows.Add(firstName, lastName, address, city);

                        //using (MySqlCommand myCmd = new MySqlCommand(Command, mConnection))
                        //{
                        //    myCmd.CommandType = CommandType.Text;
                        //    myCmd.Parameters.AddWithValue("@FirstName", item.firstname);
                        //    myCmd.Parameters.AddWithValue("@LastName", item.lastname);
                        //    myCmd.Parameters.AddWithValue("@Address", item.address);
                        //    myCmd.Parameters.AddWithValue("@City", item.city);
                        //    myCmd.ExecuteNonQuery();
                        //}
                        counter++;
                        if(counter == array.length())
                        {
                            string tempCsv = @"dump.csv";
                            using (StreamWriter w = new StreamWriter(tempCsv))
                            {
                                Rfc4180Writer.WriteDataTable(temporaryTable, w, false);
                            }
                            var mBulk = new MySqlBulkLoader(mConnection);
                            mBulk.TableName = "persons";
                            mBulk.FileName = tempCsv;
                            mBulk.FieldTerminator = ",";
                            mBulk.FieldQuotationCharacter = '"';
                            mBulk.Load();
                            System.IO.File.Delete(tempCsv);
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

public static class Rfc4180Writer
{
    public static void WriteDataTable(DataTable sourceTable, TextWriter writer, bool includeHeaders)
    {
        if (includeHeaders)
        {
            IEnumerable<String> headerValues = sourceTable.Columns
                .OfType<DataColumn>()
                .Select(column => QuoteValue(column.ColumnName));

            writer.WriteLine(String.Join(",", headerValues));
        }

        IEnumerable<String> items = null;

        foreach (DataRow row in sourceTable.Rows)
        {
            items = row.ItemArray.Select(o => QuoteValue(o?.ToString() ?? String.Empty));
            writer.WriteLine(String.Join(",", items));
        }

        writer.Flush();
    }

    private static string QuoteValue(string value)
    {
        return String.Concat("\"",
        value.Replace("\"", "\"\""), "\"");
    }
}