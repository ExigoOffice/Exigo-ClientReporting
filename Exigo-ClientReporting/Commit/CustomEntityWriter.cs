using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Reflection;

namespace Exigo.ClientReporting
{

    [global::System.AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class SqlCommitEntityMap : Attribute
    {
        public string TableName;
        public string PrimaryKey;
    }

    public class CustomEntityWriter
    {
        private string _connectionString;
        private string _schemaName;

        public CustomEntityWriter(string connectionString, string schemaName)
        {
            _connectionString = connectionString;
            _schemaName = schemaName;
        }

        
        private Dictionary<string, int> _viewsToFlip; // set as null so we can flip on first occurance
        //public override void CycleFinished(DataCache cache)
        //{
        //    if (_viewsToFlip != null)
        //    {
        //        using (var conn = new SqlConnection(_connectionString))
        //        {
        //            conn.Open();
        //            foreach (var tableName in _viewsToFlip.Keys)
        //            {
        //                FlipPeriodViews(conn, _viewsToFlip[tableName], tableName);
        //            }
        //        }
        //    }
        //}

        //write code to create table flip switch if it doesn't already exist

        byte GetCurrentPeriodWriteIndex(SqlConnection conn, int companyID, string tableName)
        {
            //--> Get the current live id
            SqlCommand cmd = new SqlCommand($"Select LiveID From {_schemaName}.TableFlipSwitch Where CompanyID=@CompanyID and TableName=@TableName", conn);
            cmd.Parameters.Add("@CompanyID", SqlDbType.Int).Value = companyID;
            cmd.Parameters.Add("@TableName", SqlDbType.VarChar, 200).Value = tableName;
            var o = cmd.ExecuteScalar();
            if (o == null) throw new Exception(tableName + " not setup properly for snapshots");
            return (byte)cmd.ExecuteScalar();

        }

        void FlipPeriodViews(SqlConnection conn, int companyID, string tableName)
        {
            byte current = GetCurrentPeriodWriteIndex(conn, companyID, tableName);

            string readTable = _schemaName + "." + tableName + "Table";
            string writeTable = tableName + "." + "Table";
            byte newCurrent = 1;
            if (current == 1)
            {
                readTable = readTable + "2";
                writeTable = writeTable + "1";
                newCurrent = 2;
            }
            else
            {
                readTable = readTable + "1";
                writeTable = writeTable + "2";
                newCurrent = 1;
            }

            //--> Flip the views
            var cmd = new SqlCommand(@"
            alter view " + tableName + @"
            as
            Select * from " + readTable, conn);
            cmd.ExecuteNonQuery();

            //--> Update the switch 
            cmd = new SqlCommand($@"Update {_schemaName}.TableFlipSwitch set LiveID=@NewCurrentID Where CompanyID=@CompanyID and TableName=@TableName", conn);
            cmd.Parameters.Add("@CompanyID", SqlDbType.Int).Value = companyID;
            cmd.Parameters.Add("@TableName", SqlDbType.VarChar, 200).Value = tableName;
            cmd.Parameters.Add("@NewCurrentID", SqlDbType.TinyInt).Value = newCurrent;
            cmd.ExecuteNonQuery();

        }

        public void SaveCustomPeriodTable<T>(SqlConnection conn, int companyID, object o, string tableName, string keyName, int periodTy, int periodID)
        {
            if (_viewsToFlip == null) _viewsToFlip = new Dictionary<string, int>();

            if (!_viewsToFlip.ContainsKey(tableName))
            {
                _viewsToFlip.Add(tableName, companyID);

                //If this is the first cycle we need to 
                //  a) create the tables if they don't exist
                //  b) truncate the write only table

                EnsurePeriodTablePairExists(conn, o, companyID, tableName, keyName);
                TruncateCurrentPeriodWriteTable(conn, tableName);
            }

            ////this is run each time
            //GenericInvoker invoker = DynamicMethods.GenericMethodInvokerMethod(this.GetType(), "GetPeriodCustomBulkCopyReader",
            //    new Type[] { listWrapper.ItemType },                    //type of generic
            //    new Type[] { typeof(int), typeof(int), typeof(int), listWrapper.ListType, typeof(DataTable) }  //types of the param
            //    );

            //DoBulk((SqlBulkCopyReader)invoker(this, companyID, periodTy, periodID, listWrapper.List,
            //    GetTableFieldDefinitions(conn, tableName)),
            //    GetCurrentPeriodWriteTable(conn, companyID, tableName), conn);

            string tableNameWithSchema = _schemaName + "." + tableName;
            CustomEntityForPeriodBulkCopyReader<T> rd = new CustomEntityForPeriodBulkCopyReader<T>(periodTy, periodID, o as ICollection<T>, GetTableFieldDefinitions(conn, tableNameWithSchema));

            DoBulk((SqlBulkCopyReader)rd, tableNameWithSchema, conn);

            //the views will be flipped in the CycleFinished method call
        }

        internal void DoBulk(SqlBulkCopyReader rd, string tableName, SqlConnection conn)
        {
            try
            {
                using (var bc = new SqlBulkCopy(conn))
                {
                    bc.DestinationTableName = tableName;
                    bc.BulkCopyTimeout = 6000;
                    bc.WriteToServer(rd);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error Bulk Inserting into " + tableName + ": " + ex.Message);
            }
            finally
            {
                rd.Close();
            }
        }


        void TruncateCurrentPeriodWriteTable(SqlConnection conn,  string tableName)
        {
            string currentWriteTable = GetCurrentPeriodWriteTable(conn, tableName);

            var cmd = new SqlCommand("Truncate Table " + _schemaName + "." + currentWriteTable, conn);
            cmd.CommandTimeout = 1200; // mins
            cmd.ExecuteNonQuery();
        }

        private string GetCurrentPeriodWriteTable(SqlConnection conn, string tableName)
        {
            byte current = GetCurrentPeriodWriteIndex(conn,  tableName);
            string currentWriteTable = tableName + "Table" + ((current == 1) ? "2" : "1");
            return currentWriteTable;
        }

        byte GetCurrentPeriodWriteIndex(SqlConnection conn, string tableName)
        {
            //--> Get the current live id
            SqlCommand cmd = new SqlCommand($@"Select LiveID From {_schemaName}.TableFlipSwitch Where TableName=@TableName", conn);
            cmd.Parameters.Add("@TableName", SqlDbType.VarChar, 200).Value = tableName;
            var o = cmd.ExecuteScalar();
            if (o == null) throw new Exception(tableName + " not setup properly ");
            return (byte)cmd.ExecuteScalar();

        }

        //private void EnsurePeriodTablePairExists(SqlConnection conn, EntityListWrapper listWrapper, int companyID, string tableName, string keyName)
        private void EnsurePeriodTablePairExists(SqlConnection conn, object obj, int companyID, string tableName, string keyName)
        {
            //string readTableName = _schemaName + "." + tableName + "Table1";
            //string writeTableName = _schemaName + "." + tableName + "Table2";

            string readTableName = tableName + "Table1";
            string writeTableName = tableName + "Table2";

            StringBuilder sb = new StringBuilder();
            //var props = listWrapper.ItemType.GetProperties();

            Type[] arguments = obj.GetType().GetGenericArguments();

            //var props = obj.GetType().GetProperties();

            
            //dictionary
            var props = arguments[arguments.Length - 1].GetProperties();

            var cmd = new SqlCommand("select case when exists(select * from sysobjects where type = 'v' and UID = schema_ID('Reporting') and name = '" + tableName + "') then 1 else 0 end", conn);
            var exists = Convert.ToBoolean(cmd.ExecuteScalar());

            if (!exists)
            {
                sb.Append($@"
                    if not exists(select * from {_schemaName}.TableFlipSwitch where TableName='{tableName}')
                        Insert {_schemaName}.TableFlipSwitch(TableName, LiveID) values('{tableName}', 1)");
                CreatePeriodTableScript(sb, readTableName, keyName, props);
                CreatePeriodTableScript(sb, writeTableName, keyName, props);

                cmd = new SqlCommand(sb.ToString(), conn);
                cmd.ExecuteNonQuery();

                cmd = new SqlCommand(@"
                create view " + _schemaName + "." + tableName + @"
                as
                Select * from " + _schemaName + "." + readTableName, conn);
                cmd.ExecuteNonQuery();
            }

            //if (PrimaryKeyUtil.PrimaryKeyEnforced(companyID))
            //{
            //    sb = new StringBuilder();
            //    sb.Append(PrimaryKeyUtil.GetEnsurePrimaryKeySql(companyID, readTableName));
            //    sb.Append(PrimaryKeyUtil.GetEnsurePrimaryKeySql(companyID, writeTableName));
            //    cmd = new SqlCommand(sb.ToString(), conn);
            //    cmd.CommandTimeout = 3000;
            //    cmd.ExecuteNonQuery();
            //}

            string readTableWithSchema = _schemaName + "." + readTableName;

            var dt = GetTableFieldDefinitions(conn, readTableWithSchema);

            //we want to make sure if the object has changed we don't go any further.
            int offset = 2;
            //if (PrimaryKeyUtil.PrimaryKeyEnforced(companyID))
            //{
            //    offset++;
            //}


            if ((dt.Columns.Count - offset) != props.Length)
            {
                //--> Give it one more chance if it was a primary key inserted
                if (dt.Columns[dt.Columns.Count - 1].DataType.Equals(typeof(Guid)))
                    offset++;


                if ((dt.Columns.Count - offset) != props.Length)
                {

                    throw new Exception("Columns on existing table " + tableName +
                                        " do not match properties on class " + //listWrapper.ItemType.Name + " dt: " +
                                        (dt.Columns.Count - offset) + " props: " + props.Length);

                }
            }

            //--> it seems pulling the properties do not always come in the same order
            for (int i = 2; i < dt.Columns.Count; i++)
            {
                if (!props.Any(pi => pi.Name == dt.Columns[i].ColumnName) && !dt.Columns[i].DataType.Equals(typeof(Guid)))
                    throw new Exception("Could not find property " + dt.Columns[i].ColumnName + " for table " + tableName);
            }

        }


        DataTable GetTableFieldDefinitions(SqlConnection conn, string tableName)
        {
            DataTable dt = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter("Select top 0 * from " + tableName, conn);
            da.Fill(dt);

            return dt;
        }

        private void CreatePeriodTableScript(StringBuilder sb, string tableName, string keyName, PropertyInfo[] props)
        {
            sb.Append($@"
                create table {_schemaName}.{tableName}
                (");

            //sb.Append("CompanyID int not null, PeriodTy int not null, PeriodID int not null, ");
            sb.Append("PeriodTy int not null, PeriodID int not null, ");

            for (int i = 0; i < props.Length; i++)
            {
                var pi = props[i];

                if (i > 0) sb.Append(",");

                sb.AppendFormat(" {0} ", pi.Name);

                if (pi.PropertyType == typeof(int)) sb.Append("int");
                else if (pi.PropertyType == typeof(decimal)) sb.Append("money");
                else if (pi.PropertyType == typeof(byte)) sb.Append("tinyint");
                else if (pi.PropertyType == typeof(short)) sb.Append("smallint");
                else if (pi.PropertyType == typeof(DateTime)) sb.Append("datetime");
                else if (pi.PropertyType == typeof(bool)) sb.Append("bit");
                else sb.Append("nvarchar(max)");

                sb.Append(" not null");
            }

            //if (!props.Any(p => p.Name == keyName))
            //    throw new Exception("Could not find primary key " + keyName + " in property for " + tableName);

            if (!string.IsNullOrEmpty(keyName))
            {
                keyName = "," + keyName;
            }

            sb.AppendFormat(@"
                )
                create clustered index IX_{0} on {1}.{0} 
                (
                    PeriodTy, PeriodID " + keyName + @"
                )
                WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
            ", tableName, _schemaName);

        }
    }
}
