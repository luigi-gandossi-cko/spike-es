using Nest;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace GeneratorDataES
{
    class Program
    {
        public static SqlConnection conn = new SqlConnection("Server=10.17.20.20;database=ADMINISTRATION;uid=administration_user;pwd=administration_user;" +
          "ApplicationIntent=ReadOnly;");
        public static ElasticClient client = new ElasticClient(new ConnectionSettings(new Uri("http://localhost:9200/")));

        static void Main(string[] args)
        {
            conn.Open();
            List<TransactionTable> transactionList = new List<TransactionTable>();
            var randomTest = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstwxyz0123456789";
            var qstartDate = new DateTime(2017, 07, 01, 0, 00, 00);
            var fstartDate = new DateTime(2016, 01, 01, 0, 00, 00);
            TimeSpan qdateRange = DateTime.Now - qstartDate;


            List<String> action = new List<string> { "INCLUDE", "EXCLUDE" };
            List<String> column = new List<string> { "ccName", "ccNumber", "currencySymbol", "customerEmail", "id", "scheme", "product", "responseCode", "timestamp", "trackId", "amount" };
            List<String> Operator = new List<string> { "CONTAINS", "BEGINSWITH", "ENDSWITH", "GREATERTHAN", "GREATERTHANEQUAL", "LESSTHAN", "LESSTHANEQUAL", "EQUALS" };

            for (var i = 0; i < 100000; i++)
            {
                List<CustomFilter> listFilter = new List<CustomFilter>();
                //TABLE RANDOM
                //random query date
                TimeSpan qSpan = new TimeSpan(randomTest.Next(0, (int)qdateRange.Days), randomTest.Next(0, 24), randomTest.Next(0, 60), 0);
                DateTime qDate = qstartDate + qSpan;
                //random from date
                TimeSpan fdateRange = qDate - fstartDate;
                TimeSpan fSpan = new TimeSpan(randomTest.Next(0, (int)fdateRange.Days), randomTest.Next(0, 24), randomTest.Next(0, 60), 0);
                DateTime fDate = fstartDate + fSpan;
                //random search value
                String search = new string(Enumerable.Repeat(chars, randomTest.Next(0, 6)).Select(s => s[randomTest.Next(s.Length)]).ToArray());

                //CUSTOM FILTER RANDOM
                var randNoFilter = randomTest.Next(0, 5);
                for (var k = 0; k < randNoFilter; k++)
                {
                    CustomFilter customFilter = new CustomFilter(action[randomTest.Next(0, action.Count)], column[randomTest.Next(0, column.Count)], Operator[randomTest.Next(0, Operator.Count)], search);
                    listFilter.Add(customFilter);
                }

                TransactionTable table = new TransactionTable(qDate, randomTest.Next(1, 5000), fDate, qDate, search, Convert.ToByte(randNoFilter), listFilter);
                SetEntitybyID(table, randomTest.Next(2, 5), randomTest.Next(100000, 100002));
                client.Index(table, s => s.Index("hub-query").Type("TransactionTable"));
                conn.Close();
            }
        }

        public static void SetEntitybyID(TransactionTable temp, long entityTypeId, long entityId)
        {
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader = null;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            switch (entityTypeId)
            {
                case 2:
                    cmd.CommandText = "select ma.merchantaccountid, ma.merchantaccountname, null, null, null, null " +
                        "from merchantaccount ma " +
                        "where ma.merchantaccountid = " + entityId;
                    temp.EntityType = "merchant";
                    reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        temp.MerchantName = reader.GetString(1);
                    }
                    break;
                case 3:
                    cmd.CommandText = "	select ma.merchantaccountid, ma.merchantaccountname, b.businessid, b.businessname, null, null " +
                        "from Business b " +
                        "inner join merchantaccount ma on ma.merchantaccountid = b.merchantaccountid " +
                        "where b.businessid = " + entityId;
                    temp.EntityType = "business";
                    reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        temp.MerchantName = reader.GetString(1);
                        temp.BusinessName = reader.GetString(3);
                    }
                    break;
                case 4:
                    cmd.CommandText = "select ma.merchantaccountid, ma.merchantaccountname, b.businessid, b.businessname, c.channelid, c.channelname " +
                        "from Channel c " +
                        "inner join business b on b.businessid = c.businessid " +
                        "inner join merchantaccount ma on ma.merchantaccountid = b.merchantaccountid " +
                        "where c.channelID = " + entityId;
                    temp.EntityType = "channel";
                    reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        temp.MerchantName = reader.GetString(1);
                        temp.BusinessName = reader.GetString(3);
                        temp.ChannelName = reader.GetString(5);
                    }
                    break;
                default:
                    break;
            }

            reader.Close();
        }
    }

    class TransactionTable
    {
        public DateTime QueryDate { get; set; }

        public long ElapsedTime { get; set; }

        public DateTime FromDate { get; set; }

        public DateTime ToDate { get; set; }

        public String EntityType { get; set; }

        public String MerchantName { get; set; }

        public String BusinessName { get; set; } = null;

        public String ChannelName { get; set; } = null;

        public String SearchValue { get; set; }

        public byte HaveFilter { get; set; }

        public List<CustomFilter> CustomFilter { get; set; }

        public TransactionTable(DateTime qdate, long elapsed, DateTime fdate, DateTime tdate, String search, byte haveFilter, List<CustomFilter> filter)
        {
            QueryDate = qdate;
            ElapsedTime = elapsed;
            FromDate = fdate;
            ToDate = tdate;
            SearchValue = search;
            HaveFilter = haveFilter;
            CustomFilter = filter;
        }
    }
    class CustomFilter
    {
        public String Action { get; set; }

        public String Column { get; set; }

        public String Operator { get; set; }

        public String Value { get; set; }

        public CustomFilter(String act, String col, String Ope, String Val)
        {
            Action = act;
            Column = col;
            Operator = Ope;
            Value = Val;
        }
    }
}

