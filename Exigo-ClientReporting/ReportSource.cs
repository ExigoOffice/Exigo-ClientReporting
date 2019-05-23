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
    /*
     * TODO: 
     * Finish Volumes 
     * Add Order Source
     * PeriodRankScores -- or custom source?
     * 
     * Functionality for Retrieving Custom Sources
     *      Supply Sql (and Mapping?)
     * Functionality for Saving Custom sources 
     *      with Primary Key (Allow Multiple key keys?)
     * 
     */


    public class CustomerList : Dictionary<int, Customer>
    {
        public DateTime Modified { get; set; }
    }

    public class OrderList : Dictionary<int, Order>
    {
        public DateTime Modified { get; set; }
    }

    public class VolumeList : Dictionary<int, Volume>
    {
        public DateTime Modified { get; set; }
    }

    public class PreviousVolumeList : Dictionary<int, Volume>
    {
        public DateTime Modified { get; set; }
    }

    public class ReportSource
    {
        public ReportSource(string ConnectionString) {
            _connectionString = ConnectionString;
        }
        private string _connectionString;

        public Tree UnilevelTree { get; private set; }
        public Tree EnrollerTree { get; private set; }
        public Tree BinaryTree { get; private set; }
        
        public CustomerList Customers { get; private set; }
        public VolumeList Volumes { get; private set; }
        public VolumeList PreviousVolumes { get; private set; }
        public OrderList Orders { get; private set; }

        public ReportingConfiguration Configuration { get; set; }
        
        public int PeriodID { get; set; }
        //may want to expand to support multiple period types
        public int PeriodTypeID { get; set; }

        public void Initialize()
        {
            UnilevelTree = new Tree();
            EnrollerTree = new Tree();
            BinaryTree = new Tree();
            Customers = new CustomerList() { Modified = new DateTime(2000, 1, 1) };
            Orders = new OrderList() { Modified = new DateTime(2000, 1, 1) };
            Volumes = new VolumeList();
            PreviousVolumes = new VolumeList();

        }

        public void Load(DateTime MinOrderDate, DateTime maxOrderDate)
        {

            UpdateEnrollerTree(EnrollerTree);
            UpdateUniLevelTree(UnilevelTree);
            UpdateBinaryTree(BinaryTree);
            UpdateCustomers(Customers);
            UpdateVolumes(Volumes, PeriodTypeID, PeriodID, true);
            UpdateOrders(Orders, MinOrderDate, maxOrderDate, null, null);

            LinkCustomersToObjects();
        }

        public void LoadPreviousVolumes()
        {
            UpdateVolumes(PreviousVolumes, PeriodTypeID, PeriodID - 1, false);
        }

        public void Reload()
        {

        }

        internal void BuildTree(Tree tree, IDataReader rd)
        {
            PopulateNode(tree.RootNode, rd);

            Node parentNode = tree.RootNode;

            while (rd.Read())
            {
                //this will climb up to find the parent to put this child
                //this assums it is ordered correctly
                while (parentNode.NodeID != rd.GetInt32(1))
                {
                    //TODO: we need an endless loop watcher
                    if (parentNode.Parent == null)
                        throw new Exception("We did not expect a null parent");

                    parentNode = parentNode.Parent;
                }

                //Node node = Node.Pool.GetObject();
                Node node = new Node();
                PopulateNode(node, rd);
                tree.InsertNode(node, parentNode);

                //we'll set the parent node to the new one so it can write below it if need bee
                parentNode = node;
            }
        }

        void PopulateNode(Node node, IDataReader rd)
        {
            node.NodeID = rd.GetInt32(0);
            node.ParentID = rd.GetInt32(1);
            node.CustomerID = rd.GetInt32(2);
            node.NestedLevel = rd.GetInt32(3);
            node.Lft = rd.GetInt32(4);
            node.Rgt = rd.GetInt32(5);
            node.Placement = rd.GetInt32(6);

            if (rd.FieldCount >= 8)
                node.Instance = rd.GetInt32(7);
        }

        public static DateTime GetResourceModified(SqlConnection conn, params string[] tableNames)
        {
            //There is an interesting effect here...if the table has never been updates since sql was restarted, then 
            //the last_user_update will be null, and will return the dbo.getlocaldate() so data will always pull from sql...
            //chances are this is fine as the only tables which will not have this value
            //will be small ones like bonus...but we *could* handle differently...(ie if there are NO rows
            SqlCommand cmd = null;
            if (tableNames.Length == 1)
            {
                cmd = new SqlCommand(@"
                    select max(last_user_update),count(*),dbo.getlocaldate()
                    from sys.dm_db_index_usage_stats 
                    where database_id=db_id() 
                    and object_id=object_id(@table);
                    ",
                    conn);
                cmd.Parameters.Add("@table", SqlDbType.NVarChar, 50).Value = tableNames[0];
            }
            else
            {
                StringBuilder sb = new StringBuilder(@"
                    select max(last_user_update),count(*),dbo.getlocaldate()
                    from sys.dm_db_index_usage_stats 
                    where database_id=db_id() 
                    and object_id in (");

                for (int i = 0; i < tableNames.Length; i++)
                {
                    if (i > 0)
                        sb.Append(",");
                    sb.Append("object_id(N'" + tableNames[i] + "')");
                }
                sb.Append(")");
                cmd = new SqlCommand(sb.ToString(), conn);
            }

            using (IDataReader rd = cmd.ExecuteReader())
            {
                rd.Read();

                //if we have a null max date but we have a count, then the table hasn't been updated since sql was restarted
                //so let's give it a fixed date....
                if (rd.IsDBNull(0) && rd.GetInt32(1) > 0)
                {
                    return new DateTime(1976, 12, 8);
                }
                else if (rd.IsDBNull(0))
                {
                    return rd.GetDateTime(2); //in this case we can't find a table so always return server time
                }
                else
                {
                    return rd.GetDateTime(0); //cool, we found what we want, return this.
                }
            }
        }

        public  bool UpdateEnrollerTree(Tree tree)
        {
            SqlConnection conn = GetConnection();
            IDataReader rd = null;
            try
            {
                DateTime lastModified = GetResourceModified(conn, "EnrollerTree");
                if (lastModified > tree.Modified)
                {
                    BeginTrace("UpdateEnrollerTree");

                    SqlCommand cmd = new SqlCommand(@"
					Select  ID			= CustomerID, 
							ParentID	= EnrollerID,
							CustomerID	= CustomerID,
							NestedLevel,
							lft,
							rgt,
							Placement = 0
					From EnrollerTree
					Order by lft
                    ", conn);
                    rd = cmd.ExecuteReader();

                    tree.Clear();

                    if (rd.Read())
                        BuildTree(tree, rd);

                    tree.Modified = lastModified;

                    EndTrace("UpdateEnrollerTree", "EnrollerTree Refresh");

                    return true;
                }
            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }
            return false;
        }

        public bool UpdateUniLevelTree(Tree tree)
        {
            SqlConnection conn = GetConnection();
            IDataReader rd = null;
            try
            {
                DateTime lastModified = GetResourceModified(conn, "UnilevelTree");
                if (lastModified > tree.Modified)
                {

                    BeginTrace("UpdateUniLevelTree");

                    SqlCommand cmd = new SqlCommand(@"
					Select  ID			= CustomerID, 
							ParentID	= SponsorID,
							CustomerID	= CustomerID,
							NestedLevel,
							lft,
							rgt,
							Placement
					From UniLevelTree
					Order by lft
                    ", conn);
                    rd = cmd.ExecuteReader();

                    tree.Clear();

                    if (rd.Read())
                        BuildTree(tree, rd);

                    tree.Modified = lastModified;

                    EndTrace("UpdateUniLevelTree", "UniLevelTree Refresh");

                    return true;
                }
            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }
            return false;
        }

        public bool UpdateBinaryTree(Tree tree)
        {
            SqlConnection conn = GetConnection();
            IDataReader rd = null;
            try
            {
                DateTime lastModified = GetResourceModified(conn, "BinaryTree");
                if (lastModified > tree.Modified)
                {

                    BeginTrace("UpdateBinaryTree");

                    SqlCommand cmd = new SqlCommand(@"
					Select  ID			= CustomerID, 
							ParentID	= ParentID,
							CustomerID	= CustomerID,
							NestedLevel,
							lft,
							rgt,
							cast(Placement as int)
					From BinaryTree
					Order by lft
                    ", conn);
                    rd = cmd.ExecuteReader();

                    tree.Clear();

                    if (rd.Read())
                        BuildTree(tree, rd);

                    tree.Modified = lastModified;

                    EndTrace("UpdateBinaryTree", "BinaryTree Refresh");

                    return true;
                }
            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }
            return false;
        }

        public bool UpdateOrders(OrderList orders, DateTime MinOrderDate, DateTime MaxOrderDate, string filter, params object[] paramList)
        {
            SqlConnection conn = GetConnection();
            IDataReader rd = null;
            try
            {

                BeginTrace("UpdateOrders");

                bool isUpdate = true;

                string updateSql = "";
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = conn;
                cmd.CommandTimeout = 1000;

                
                //if the usual engine is pulling it
                if (filter == null)
                {
                    filter = @"
                        where    orders.OrderDate >= @StartDate
                        and 	orders.OrderDate < (@EndDate+1)
                    ";

                    cmd.Parameters.Add("@StartDate", SqlDbType.DateTime).Value = MinOrderDate;
                    cmd.Parameters.Add("@EndDate", SqlDbType.DateTime).Value = MaxOrderDate;

                    if (orders.Count == 0)
                        isUpdate = false;

                    if (isUpdate && orders.Modified > DateTime.MinValue)
                    {
                        updateSql = " and orders.ModifiedDate > @LastPullDate";
                        cmd.Parameters.Add("@LastPullDate", SqlDbType.DateTime).Value = orders.Modified.AddMinutes(-5);
                    }
                }
                //this is only called if someone is pulling a custom order query
                else
                {
                    filter = " where " + filter;
                    object[] a = new object[paramList.Length];
                    for (int i = 0; i < paramList.Length; i++)
                    {
                        a[i] = "@Param" + i.ToString();
                    }
                    filter = string.Format(filter, a);
                    for (int i = 0; i < paramList.Length; i++)
                    {
                        cmd.Parameters.AddWithValue("@Param" + i.ToString(), paramList[i]);
                    }
                    isUpdate = false;
                }

                cmd.CommandText = @"
					Select	
						orders.OrderID, 
						OrderTy = isnull(oo.OrderTypeID, orders.OrderTypeID),
						CustomerID = isnull(orders.TransferToCustomerID, orders.CustomerID),
						lower(orders.CurrencyCode),
						orders.OrderDate,
						orders.CommissionableVolumeTotal,
						orders.BusinessVolumeTotal,
						orders.SubTotal,
						orders.PriceTypeID,
                        ReturnedOrderDate = oo.OrderDate,
                        ReturnedOrderID   = oo.OrderID,                 --10
                        oOrderStatusTy          = orders.OrderStatusID, --11
                        oWarehouseID            = cast(orders.WarehouseID as int),  --12
                        orders.ModifiedDate                                         --13
					From 	Orders orders
                    left Join Orders oo 		-- Original Order (for returns)
						on orders.ReturnOrderID    = oo.OrderID
                    " + filter + updateSql + @"
                    order by Orders.OrderID--, OrderDetail.OrderLine
                    ";

                int orderstatusPosition = 11;

                //debug:

                //string log = "";
                //foreach (SqlParameter prm in cmd.Parameters)
                //    log += prm.ParameterName + "=" + prm.Value.ToString() + ", ";

                //Cache.Feedback.Trace("Order selection " + log);

                rd = cmd.ExecuteReader();

                int id = -1;    //for order id loop
                short ol = -1;  //for order line loop                    

                Order o = null;

                int cnt = 0;

                while (rd.Read())
                {
                    if (id != rd.GetInt32(0))
                    {


                        o = null;
                        int orderID = rd.GetInt32(0);

                        //we want to filter out un accepted orders
                        if (rd.GetInt32(orderstatusPosition) < 7 || rd.GetInt32(orderstatusPosition) == 10)
                        {
                            id = -1; //reset the ID..it will evaluate it for every child item
                            if (isUpdate && orders.ContainsKey(orderID))
                                orders.Remove(orderID);

                            continue;
                        }

                        //if the company filters out non iscommissionable orders 
                        //if (Cache.Config.IncludeNonCommissionableOrders == false && rd.GetBoolean(21) == false)
                        //{
                        //    id = -1; //reset the ID..it will evaluate it for every child item
                        //    if (isUpdate && orders.ContainsKey(orderID))
                        //        orders.Remove(orderID);

                        //    continue;
                        //}


                        cnt++;

                        if ((isUpdate && !orders.TryGetValue(orderID, out o))
                            || (!isUpdate))
                        {
                            o = new Order();
                            o.OrderID = orderID;
                            orders.Add(o.OrderID, o);
                        }
                        else
                        {
                            //reset all internal vars
                            //Order.Clear(o);
                            o = new Order();
                            o.OrderID = orderID;
                        }

                        
                        o.OrderTypeID = rd.GetInt32(1);
                        o.CustomerID = rd.GetInt32(2);
                        o.CurrencyCode = rd.GetString(3).ToLower();
                        o.OrderDate = rd.GetDateTime(4);
                        o.CV = rd.GetDecimal(5);
                        o.BV = rd.GetDecimal(6);
                        o.SubTotal = rd.GetDecimal(7);
                        o.PriceTypeID = rd.GetInt32(8);
                        //if (!rd.IsDBNull(9))
                        //{
                        //    o.ReturnedOrderDate = rd.GetDateTime(9);
                        //    o.ReturnedOrderID = rd.GetInt32(10);
                        //}
                        o.OrderStatusID = rd.GetInt32(11);
                        o.WarehouseID = rd.GetInt32(12);

                        id = o.OrderID;
                        ol = -1;

                        if (rd.GetDateTime(13) > orders.Modified)
                            orders.Modified = rd.GetDateTime(13);

                        Customer c = null;
                        if (Customers.TryGetValue(o.CustomerID, out c)){
                            o.Customer = c;
                        }
                        else
                        {
                            orders.Remove(o.OrderID);
                        }
                    }

                    #region commented out detail
                    //if (ol != rd.GetInt16(40)) //we are a new order line
                    //{
                    //    OrderDetail det = OrderDetail.Pool.GetObject();
                    //    det.ItemCode = rd.GetString(26);
                    //    det.Quantity = rd.GetDecimal(27);
                    //    det.CommissionableVolume = rd.GetDecimal(28);
                    //    det.BusinessVolume = rd.GetDecimal(29);
                    //    det.Other1 = rd.GetDecimal(30);
                    //    det.Other2 = rd.GetDecimal(31);
                    //    det.Other3 = rd.GetDecimal(32);
                    //    det.Other4 = rd.GetDecimal(33);
                    //    det.Other5 = rd.GetDecimal(34);
                    //    det.Other6 = rd.GetDecimal(35);
                    //    det.Other7 = rd.GetDecimal(36);
                    //    det.Other8 = rd.GetDecimal(37);
                    //    det.Other9 = rd.GetDecimal(38);
                    //    det.Other10 = rd.GetDecimal(39);
                    //    det.PriceTotal = rd.GetDecimal(45);

                    //    det._order = o;
                    //    o._details.Add(det);

                    //    ol = rd.GetInt16(40);
                    //}

                    ////if we have kit items, let's put them in here as well
                    //if (!rd.IsDBNull(41))
                    //{
                    //    OrderDetail det = OrderDetail.Pool.GetObject();
                    //    det.IsKitDetail = true;
                    //    det.ItemCode = rd.GetString(41);
                    //    det.Quantity = rd.GetDecimal(42);
                    //    det.CommissionableVolume = rd.GetDecimal(43);
                    //    det.BusinessVolume = rd.GetDecimal(44);

                    //    det._order = o;
                    //    o._details.Add(det);
                    //}
                    #endregion

                }



                EndTrace("UpdateOrders", $"Order Refresh with ({cnt:#,#}) records.");
                return true;

            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }
            return false;
        }


        public bool UpdateEntities<EntityType>(ICollection<EntityType> list, params object[] arg) where EntityType : class, new()
        {
            SqlConnection conn = GetConnection();
            //IDataReader rd = null;
            SqlDataReader rd = null;
            Type type = typeof(EntityType);
            try
            {
                bool doit = true;

                DateTime lastModified = DateTime.MinValue;

                var atts = type.GetCustomAttributes(typeof(SqlSourceEntityMap), false);
                if (atts.Length == 0) throw new Exception("Expected SqlSourceEntityMap on entity type " + type.Name);
                var config = atts[0] as SqlSourceEntityMap;

                //if (!string.IsNullOrEmpty(config.InvalidationTables))
                //{
                //    lastModified = GetResoureModified(conn, config.InvalidationTables.Split(','));

                //    if (lastModified <= list.Modified) //if our last modified is the same or less then the list we don't need to pull it again
                //        doit = false;
                //}

                if (doit)
                {
                    int cnt = 0;

                    //TODO: handle modified date and partial updates...
                    BeginTrace("Update" + type.Name);

                    SqlCommand cmd = GetParamCommand(config.SqlCommandText, arg);
                    cmd.Connection = conn;
                    cmd.CommandTimeout = 1000;

                    list.Clear();

                    rd = cmd.ExecuteReader();

                    Dictionary<int, PropertyInfo> map = new Dictionary<int, PropertyInfo>();

                    //map properties to database fields (we do this before to keep the loop fast)
                    foreach (var prop in type.GetProperties())
                    {
                        for (int i = 0; i < rd.FieldCount; i++)
                        {
                            if (prop.Name.ToUpper() == rd.GetName(i).ToUpper())
                            {
                                map.Add(i, prop);
                                break;
                            }
                        }
                    }

                    //we will simply populate public properties that have the same name as field
                    while (rd.Read())
                    {
                        cnt++;
                        var o = new EntityType();
                        foreach (var i in map.Keys)
                        {
                            PropertyInfo prop = map[i];
                            prop.SetValue(o, rd[i], null);
                        }
                        list.Add(o);
                    }

                    //list.Modified = lastModified;

                    EndTrace("Update" + type.Name, type.Name + $" Refresh with {cnt:#,#} record(s)");

                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error updating " + type.Name + ": " + ex.Message, ex);
            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }

            return false;
        }

        private SqlCommand GetParamCommand(string cmdText, object[] paramList)
        {
            SqlCommand cmd = new SqlCommand();
                
            if (paramList != null && paramList.Length > 0)
            {
                if (cmdText.ToLower().IndexOf("@") > -1)
                {
                    cmd.CommandText = string.Format(cmdText, paramList);
                }
                else
                {
                    object[] a = new object[paramList.Length];
                    for (int i = 0; i < paramList.Length; i++)
                    {
                        a[i] = "@Param" + i.ToString();
                    }
                    cmd.CommandText = string.Format(cmdText, a);
                    for (int i = 0; i < paramList.Length; i++)
                    {
                        cmd.Parameters.AddWithValue("@Param" + i.ToString(), paramList[i]);
                    }
                }
            }
            else
            {
                cmd.CommandText = cmdText;
            }

            return cmd;
        }

        public bool UpdateCustomers(CustomerList customers)
        {
            SqlConnection conn = GetConnection();
            IDataReader rd = null;
            try
            {

                BeginTrace("UpdateCustomers");

                bool isUpdate = true;
                if (customers.Count == 0)
                    isUpdate = false;

                string updateSql = "";
                if (isUpdate)
                    updateSql = " Where c.ModifiedDate > @LastPullDate";


                SqlCommand cmd = new SqlCommand(@"
                Select  
	                c.CustomerID, 
	                c.CustomerTypeID,
	                c.CustomerStatusID,
	                lower(c.CurrencyCode),
	                RankID		= IsNull(c.RankID, cast(0 as tinyint)),
	                c.CreatedDate,
	                FirstName = '', /* will add back in if we need it */
                    LastName = '',
	                MainCountry,
                    ModifiedDate
                From Customers c
                "+ updateSql, conn);
                

                if (isUpdate)
                {
                    //substract 5 minutes in case anything is missed
                    cmd.Parameters.Add("@LastPullDate", SqlDbType.DateTime).Value = customers.Modified.AddMinutes(-5);
                }

                cmd.CommandTimeout = 600;

                rd = cmd.ExecuteReader();

                int cnt = 0;
                while (rd.Read())
                {
                    cnt++;

                    Customer c = null;
                    int customerID = rd.GetInt32(0);

                    if ((isUpdate && !customers.TryGetValue(customerID, out c))
                        || (!isUpdate))
                    {
                        c = new Customer();
                        c.CustomerID = rd.GetInt32(0);
                        customers.Add(c.CustomerID, c);
                    }

                    c.CustomerTypeID = rd.GetInt32(1);
                    c.CustomerStatusID = rd.GetInt32(2);
                    c.CurrencyCode = rd.GetString(3).Trim();

                    if (c.CurrencyCode == "") c.CurrencyCode = "usd";
                    if (rd.GetInt32(2) == 1) c.IsActive = true;

                    
                    c.RankID = rd.GetInt32(4);
                    c.EntryDate = rd.GetDateTime(5);
                    c.CountryCode = rd.GetString(7);

                    //c.Name = rd.GetString(10);
                    
                    if (rd.GetDateTime(9) > customers.Modified)
                        customers.Modified = rd.GetDateTime(9);
                }




                EndTrace("UpdateCustomers", $"Customer Refresh with {cnt:#,#} record(s)");

                return true;

            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }
            return false;
        }

        private SqlCommand GetVolumeCommand(SqlConnection conn, VolumeConfiguration vc, int PeriodTypeID, int PeriodID)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            
            StringBuilder sb = new StringBuilder("Select CustomerID, RankID, PaidRankID, ");
            foreach (var f in vc.VolumeFields)
            {
                sb.Append(f.FieldName + ",");
            }
            sb.Remove(sb.Length - 1, 1);
            sb.Append(" from PeriodVolumes where PeriodTypeID = @PeriodTypeID and PeriodID=@PeriodID");
            cmd.CommandText = sb.ToString();
            cmd.Parameters.Add("@PeriodTypeID",SqlDbType.Int).Value = PeriodTypeID;
            cmd.Parameters.Add("@PeriodID", SqlDbType.Int).Value = PeriodID;
            return cmd;

        }

        public bool UpdateVolumes(VolumeList volumes, int periodTy, int periodID, bool IsCurrent)
        {
            //need to decide if we're going to do partial loads/deltas

            if (Configuration == null)
                throw new Exception("Configuration object not set up");

            if (Configuration.VolumeConfig == null)
                throw new Exception("Volume Configuration not set up");


            VolumeConfiguration config = Configuration.VolumeConfig;
            int bucketCount = config.VolumeFields.Count;
            
            SqlConnection conn = GetConnection();
            IDataReader rd = null;
            try
            {
                BeginTrace("UpdateVolumes");

                volumes.Clear();

                SqlCommand cmd = GetVolumeCommand(conn, config, periodTy, periodID);

                rd = cmd.ExecuteReader();

                while (rd.Read())
                {
                    Volume sv = new Volume(bucketCount);

                    PopulateVolumeFromReader(rd, sv, config);

                    Customer c = null;
                    if (Customers.TryGetValue(sv.CustomerID, out c) && IsCurrent)
                    {
                        c.Volume = sv;
                    }
                    volumes.Add(rd.GetInt32(0), sv);
                }
                EndTrace("UpdateVolumes", $"PeriodVolume Refresh PeriodTy {periodTy}, PeriodID {periodID}");

            }
            finally
            {
                if (rd != null) rd.Close();
                ReturnConnection(conn);
            }
            return true;
        }

        private void PopulateVolumeFromReader(IDataReader rd, Volume sv, VolumeConfiguration config)
        {
            sv.CustomerID = rd.GetInt32(0);
            sv.RankID = rd.GetInt32(1);
            sv.PaidRankID = rd.GetInt32(2);

            int cnt = 3;
            foreach (var f in config.VolumeFields)
            {
                //sv[cnt-2] = rd.GetDecimal(cnt++);
                sv[f.VolumeID] = rd.GetDecimal(cnt++);
            }
        }

        void LinkCustomersToObjects()
        {
            //unlink all customer's old objects
            foreach (var cust in Customers.Values)
            {
                cust.BinaryNode = null;
                cust.EnrollerNode = null;
                cust.UnilevelNode = null;

                //since we're looping through customers we'll fix null volume records at the same time
                if (cust.Volume == null)
                    cust.Volume = new Volume();
            }

            Customer c;
            foreach (var node in EnrollerTree.AllNodes)
            {
                if (node.CustomerID != 0)
                {
                    if (Customers.TryGetValue(node.CustomerID, out c))
                    {
                        node._customer = c;
                        c.EnrollerNode = node;
                    }
                    else
                    {
                        throw new Exception("CustomerID " + node.CustomerID + " found in EnrollerTree but not in Customer");
                    }
                }
            }

            foreach (var node in UnilevelTree.AllNodes)
            {
                if (node.CustomerID != 0)
                {
                    if (Customers.TryGetValue(node.CustomerID, out c))
                    {
                        node._customer = c;
                        c.UnilevelNode = node;
                    }
                    else
                    {
                        throw new Exception("CustomerID " + node.CustomerID + " found in UniLevelTree but not in Customer");
                    }
                }
            }

            foreach (var node in BinaryTree.AllNodes)
            {
                if (node.CustomerID != 0)
                {
                    if (Customers.TryGetValue(node.CustomerID, out c))
                    {
                        node._customer = c;
                        c.BinaryNode = node;
                    }
                    else
                    {
                        throw new Exception("CustomerID " + node.CustomerID + " found in BinaryTree but not in Customer");
                    }
                }
            }


            //foreach (var order in _orderList)
            //{
            //    order._customer = _customerList[order.CustomerID];
            //}
        }


        protected SqlConnection GetConnection()
        {
            SqlConnection conn = new SqlConnection(_connectionString);
            conn.Open();
            return conn;
        }

        protected void ReturnConnection(SqlConnection conn)
        {
            conn.Close();
        }

        private void BeginTrace(string key)
        {
            BeginTrace(key, "");
        }


        private void BeginTrace(string key, string message)
        {
            
        }

        private void EndTrace(string key, string message)
        {

        }

    }
}
