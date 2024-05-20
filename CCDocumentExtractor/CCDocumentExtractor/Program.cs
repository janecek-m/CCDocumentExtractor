using DBManager.DataAccessManager;
using System.Data;
using System.Transactions;

namespace CCDocumentExtractor
{
    internal class Program
    {
        static void Main(string[] args)
        {
            RunAsDBFileExporter("User ID=;PASSWORD=;Data Source=93.185.107.65", args);

            Console.WriteLine("Pro ukonceni zmackni klavesu.");
            Console.ReadKey();
        }


        private static void RunAsDBFileExporter(string connString, string[] args)
        {
            Console.WriteLine();
            if (args.Length != 7)
            {
                Console.WriteLine("Pro export dokumentu z DB musite tuto utilitu spustit s nasledujicimi parametry: {flagIsZipped - 0 = nonZipped, 1 = zipped, 2 = zippedPackage} {dbName - string}, {tableName - string}, {columnId - string}, {columnData - string}, {recordId - string usually Guid}, {fileOutput - string, path + filename + extension}");
                return;
            }

            string isZipped = args[0];
            string dbName = args[1];
            string tableName = args[2];
            string columnId = args[3];
            string columnData = args[4];
            string recordId = args[5];
            string fileOutput = args[6];

            Console.WriteLine($"Vytahuji z DB dokument. {recordId} a ukladam do {fileOutput}");

            if (isZipped.Equals("0"))
            {
                GetDocumentDataFromDB(connString, dbName, tableName, columnData,
               columnId,
               recordId,
               fileOutput
              );
            }
            else if (isZipped.Equals("1"))
            {
                GetZippedDocumentDataFromDB(connString, dbName, tableName, columnData,
               columnId,
               recordId,
               fileOutput
               );
            }
            else if (isZipped.Equals("2"))
            {
                GetZippedPackageDocumentDataFromDB(connString, dbName, tableName, columnData,
               columnId,
               recordId,
               fileOutput
               );
            }
        }



        public static void GetZippedPackageDocumentDataFromDB(string connString, string DBName, string tableName, string columnName,
            string idColumnName, string IdRecord, string filePath)
        {
            using (
                DBTransactionManager transactionManager = new DBTransactionManager(DataProvider.SqlServer, connString))
            {
                byte[] data = GetDocumentDataFromDB(DBName, "dbo",
                    tableName, columnName, idColumnName, IdRecord,
                    transactionManager);

                var unzippedFiles = CreditFramework.CreditCompressor.Zip.DecompressPackage(new MemoryStream(data));

                SaveByteArrayToFile(unzippedFiles.First().Value, filePath);
            }
        }

        public static void GetZippedDocumentDataFromDB(string connString, string DBName, string tableName, string columnName,
            string idColumnName, string IdRecord, string filePath)
        {
            using (
                DBTransactionManager transactionManager = new DBTransactionManager(DataProvider.SqlServer, connString))
            {
                Console.WriteLine("Vytahuji soubor z DB");
                byte[] data = GetDocumentDataFromDB(DBName, "dbo",
                    tableName, columnName, idColumnName, IdRecord,
                    transactionManager);
                Console.WriteLine("Soubor uspesne vytazen. ");
                Console.WriteLine("Provadim rozbaleni ZIP souboru.");
                var unzippedFiles = CreditFramework.CreditCompressor.Zip.Decompress(data);
                Console.WriteLine("Rozbaleni ZIP souboru probehlo uspesne");
                Console.WriteLine($"Ukladam soubor do {filePath}");
                SaveByteArrayToFile(unzippedFiles, filePath);
                Console.WriteLine("Soubor ulozen uspesne");
            }
        }


        public static void GetDocumentDataFromDB(string connString, string DBName, string tableName, string columnName,
            string idColumnName, string IdRecord, string filePath)
        {
            using (
                DBTransactionManager transactionManager = new DBTransactionManager(DataProvider.SqlServer, connString))
            {
                var data = GetDocumentDataFromDB(DBName, "dbo",
                    tableName, columnName, idColumnName, IdRecord,
                    transactionManager);

                //var unzippedFiles = CreditFramework.CreditCompressor.Zip.DecompressPackage(new MemoryStream(data));

                SaveByteArrayToFile(data, filePath);
            }
        }

        /// <summary>
        /// Vrací byte[] uložené do vybraného sloupce ve vybrané tabulce jako SQL data type: Image
        /// </summary>
        /// <param name="table">název tabulky</param>
        /// <param name="columnName">název sloupce z kterého chceme vytáhnout data - obvykle documentData</param>
        /// <param name="documentID">ID dokumentu - většinou Guid, ale může být např. i int</param>
        /// <returns></returns>
        public static byte[] GetDocumentDataFromDB(string DB, string schema, string table, string columnName, string IdColumnName, string documentID, DBTransactionManager transactionManager)
        {
            IDBManager dbManager = new CustomDBManager(transactionManager);
            try
            {
                //prepareParameters(ref dbManager, colNames, values);   

                dbManager.Open();
                string sql = "select " + columnName + " from " + DB + "." + schema + "." + table + " where " +
                             IdColumnName + " = '" + documentID + "'";
                byte[] documentData = (byte[])dbManager.ExecuteScalar(CommandType.Text, sql);
                return documentData;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            finally
            {
                if (transactionManager == null)
                {
                    dbManager.Close();
                    dbManager.Dispose();
                }
            }
            return null;
        }
        public static void SaveByteArrayToFile(byte[] data, string pathToFile)
        {
            File.WriteAllBytes(pathToFile, data);
        }


        public static void Get7ZippedDocumentDataFromDB(string connString, string DBName, string tableName, string columnName,
           string idColumnName, string IdRecord, string filePath)
        {
            using (
                DBTransactionManager transactionManager = new DBTransactionManager(DataProvider.SqlServer, connString))
            {
                byte[] data = GetDocumentDataFromDB(DBName, "dbo",
                    tableName, columnName, idColumnName, IdRecord,
                    transactionManager);

                MemoryStream msResult = new MemoryStream();
                byte[] buffer = new byte[1024];
                MemoryStream ms = new MemoryStream();
                ms.Write(data, 0, data.Length);
                ms.Position = 0;

                /*
                System.util.zlib.ZInputStream zlib = new System.util.zlib.ZInputStream(ms);
                int read = 0;
                do
                {
                    read = zlib.Read(buffer, 0, buffer.Length);
                    msResult.Write(buffer, 0, read);
                } while (read > 0);
                */
                /*
                SevenZip.Compression.LZMA.Decoder decoder = new SevenZip.Compression.LZMA.Decoder();
                decoder.Code(ms, msResult, ms.Length, ms.Length, SevenZip.);
                //SevenZip.Compression.LZMA.SevenZipHelper.Decompress(byte[])
                */

                //var result = DecompressLZMA(data);

                //var unzippedFiles = Zlib.Portable.Decompress(data);

                //SaveByteArrayToFile(result, filePath);
            }
        }

    }
}
