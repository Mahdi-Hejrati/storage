using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Storage.Core;
using System.Text.RegularExpressions;

namespace Storage.Sql
{
    public class DBFieldInfo
    {
        public string TableName { get; set; }
        public string FieldName { get; set; }
        public bool isNullable { get; set; }
        public bool isIdentity { get; set; }
        public string typeName { get; set; }
    }

    public class DBTableInfo
    {
        public string name { get; internal set; }
        public List<DBFieldInfo> fields = new List<DBFieldInfo>();
    }

    public class SqlService
    {
        public static SqlService DefaultInstance = null;

        private string last_query = "";
        private int last_query_count = 0;
        public void LogExecuteSql(string sql)
        {
            if(last_query != "" && sql != last_query)
            {
                //System.IO.File.AppendAllText("tooth_sql_log.txt", $"\n{last_query} ({last_query_count})");
                last_query_count = 0;
            }
            last_query = sql;
            last_query_count++;
        }

        private readonly Dictionary<string, DBTableInfo> dbschema = new Dictionary<string, DBTableInfo>(StringComparer.OrdinalIgnoreCase);
        public DBTableInfo getSchema(string tname)
        {
            if (this.dbschema.ContainsKey(tname))
            {
                return this.dbschema[tname];
            }
            DBTableInfo ret = new DBTableInfo() { name = tname };

            var f = this.ExecuteRead(new Query(this,
@"SELECT t.name as TableName, t.kind as Kind
	,sys.columns.name as FieldName, sys.columns.is_identity as isIdentity, sys.columns.is_nullable as isNullable, sys.columns.system_type_id as typeId 
	,sys.types.name as typeName
	FROM sys.columns
	LEFT OUTER JOIN (select object_id, name, 'view' as kind from sys.views union all
		select object_id, name, 'table' as kind from sys.tables) t ON t.object_id = sys.columns.object_id
	LEFT JOIN sys.types ON sys.types.user_type_id = sys.columns.user_type_id	
WHERE t.name=@tname"), new QueryParamList("tname", tname));

            ret.fields = new List<DBFieldInfo>();

            foreach (var t in f)
            {
                if (t.getAs<string>("FieldName", "") == "rowguid")
                    continue;
                ret.fields.Add(t.MapTo<DBFieldInfo>(new DBFieldInfo()));
                //result.fields.Add(new DBFieldInfo() {
                //    TableName = t.Fields["TableName"].asString,
                //    FieldName = t.Fields["FieldName"].asString,
                //    isNullable = t.Fields["isNullable"].asBool,
                //    isIdentity = t.Fields["isIdentity"].asBool,
                //});
            }
            this.dbschema[tname] = ret;
            return ret;
        }

        public void ClearSchema()
        {
            this.dbschema.Clear();
        }

        private SqlConnection _connection;
        public SqlConnection connection
        {
            get
            {
                if (this._connection.State != ConnectionState.Open)
                    this._connection.Open();
                return this._connection;
            }
            protected set
            {
                this._connection = value;
            }
        }

        public SqlService(string cnn)
        {
            this.connection = new SqlConnection(cnn);
        }

        public SqlService(SqlConnection cn)
        {
            this.connection = cn;
        }

        public DataList<DataStore> ExecuteRead(string q, QueryParamList p = null)
        {
            return (DataList<DataStore>)ExecuteRead<DataStore>(q, p);
        }

        internal DataList<T> ExecuteRead<T>(string q, QueryParamList p = null) where T : DataStore, new()
        {
            DataList<T> ret = new DataList<T>();
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q;
                    this.LogExecuteSql(cmd.CommandText);

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    SqlDataReader r = cmd.ExecuteReader();
                    try
                    {
                        while (r.HasRows && r.Read())
                        {
                            T t = new T();
                            t.readFrom(r);
                            ret.Add(t);
                        }
                    }
                    finally
                    {
                        r.Close();
                    }
                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
            return ret;
        }


        public DataList<DataStore> ExecuteRead(IQuery q, QueryParamList p = null)
        {
            DataList<DataStore> ret = new DataList<DataStore>();
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q.getSql();
                    this.LogExecuteSql(cmd.CommandText);

                    QueryParamList pl = q.getParams();
                    if (pl != null)
                        pl.items.ForEach(t => cmd.Parameters.Add(t));

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    SqlDataReader r = cmd.ExecuteReader();
                    try
                    {
                        while (r.HasRows && r.Read())
                        {
                            ret.Add(new DataStore(r));
                        }
                    }
                    finally
                    {
                        r.Close();
                    }
                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
            return ret;
        }

        public DataTable SelectTable(IQuery q, QueryParamList p = null)
        {
            DataTable ret = new DataTable();
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q.getSql();
                    this.LogExecuteSql(cmd.CommandText);

                    QueryParamList pl = q.getParams();
                    if (pl != null)
                        pl.items.ForEach(t => cmd.Parameters.Add(t));

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    ret.Load(cmd.ExecuteReader());

                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
            return ret;
        }

        public void Execute(string q, QueryParamList p = null)
        {
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q;
                    this.LogExecuteSql(cmd.CommandText);

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
        }

        public void Execute(IQuery q, QueryParamList p = null)
        {
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q.getSql();
                    this.LogExecuteSql(cmd.CommandText);
                    QueryParamList pl = q.getParams();
                    if (pl != null)
                        pl.items.ForEach(t => cmd.Parameters.Add(t));

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
        }

        public DataStore SingleRow(string q, QueryParamList p = null)
        {
            return (DataStore)SingleRow<DataStore>(q, p);
        }

        internal T SingleRow<T>(string q, QueryParamList p = null) where T : DataStore, new()
        {
            T result = null;
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q;
                    this.LogExecuteSql(cmd.CommandText);

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    SqlDataReader r = cmd.ExecuteReader();
                    try
                    {
                        if (r.HasRows && r.Read())
                        {
                            result = new T();
                            result.readFrom(r);
                        }
                    }
                    finally
                    {
                        r.Close();
                    }
                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
            return result;
        }

        public DataStore SingleRow(IQuery q, QueryParamList p = null)
        {
            DataStore ret = null;
            lock (this.connection)
            {
                try
                {
                    SqlCommand cmd = this.connection.CreateCommand();
                    cmd.CommandText = q.getSql();
                    this.LogExecuteSql(cmd.CommandText);
                    QueryParamList pl = q.getParams();
                    if (pl != null)
                        pl.items.ForEach(t => cmd.Parameters.Add(t));

                    if (p != null)
                        p.items.ForEach(t => cmd.Parameters.Add(t));

                    SqlDataReader r = cmd.ExecuteReader();
                    try
                    {
                        if (r.HasRows && r.Read())
                        {
                            ret = new DataStore(r);
                        }
                    }
                    finally
                    {
                        r.Close();
                    }
                }
                catch (Exception ex)
                {
                    //this.connection.Close();
                    throw ex;
                    //Global.Instance.logger.Error("DBSERVICE", sql, ex.ToString());
                }
            }
            return ret;
        }

        public Field SingleValue(string q, QueryParamList p = null)
        {
            DataStore s = SingleRow(q, p);
            return s?.getAll()[0];
        }

        public Field SingleValue(IQuery q, QueryParamList p = null)
        {
            DataStore s = SingleRow(q, p);
            return s.getAll()[0];
        }


        public void TransactionBegin(string name = "")
        {
            this.Execute("BEGIN TRANSACTION " + name);
        }
        public void TransactionCommit(string name = "")
        {
            this.Execute("COMMIT TRANSACTION " + name);
        }
        public void TransactionRollback(string name = "")
        {
            this.Execute("ROLLBACK TRANSACTION " + name);
        }


        public XQ Xq(string table, DataStore store = null)
        {
            return new XQ(this, store, table);
        }




        //// select from [TableName] where marizId=[marizId] and also select from [TableName]Detail where masterId = [marizId]
        //// combine all recordes into result by fieldName, fieldValue
        //public DataStore VerticalSelect(string TableName, long marizId);
        //// Save the master and remain fields into detail TableName
        //public void VerticalSave(string TableName);


        public SelectQuery Select(string table = "") { return new SelectQuery(this).table(table); }
        public UpdateQuery Update(string table = "") { return new UpdateQuery(this).table(table); }
        public DeleteQuery Delete(string table = "") { return new DeleteQuery(this).table(table); }
        public InsertQuery Insert(string table = "") { return new InsertQuery(this).table(table); }





        
    }

    public class QueryParamList
    {
        public List<SqlParameter> items = new List<SqlParameter>();

        public QueryParamList(params object[] name_value)
        {
            this.addNameValue(name_value);
        }

        public QueryParamList(DataStore ds)
        {
            this.addFromStore(ds);
        }

        public QueryParamList addNameValue(params object[] name_value)
        {
            if (name_value.Length % 2 != 0)
            {
                throw new Exception("Invalid parameter name value list count");
            }

            List<SqlParameter> sp = new List<SqlParameter>();

            for (int i = 0; i < name_value.Length; i += 2)
            {
                this.items.Add(DbTypeInfo.newParameter(name_value[i].ToString().Trim(), name_value[i + 1]));
            }
            return this;
        }

        public QueryParamList add(string name, object value)
        {
            this.items.Add(DbTypeInfo.newParameter(name, value));
            return this;
        }
        public QueryParamList add(SqlParameter p)
        {
            this.items.Add(p);
            return this;
        }

        public QueryParamList addFromStore(DataStore ds)
        {
            ds.getAll().ForEach(t => { if (t.fieldType == FieldType.Data) add(t.fn, t.value); });
            return this;
        }
    }

    public interface IQuery
    {
        string getSql();

        QueryParamList getParams();
    }

    public enum Op
    {
        Eq, Gt, Lt, GtEq, LtEq, Between, LIKE, BetweenNoEnd, NotEq
    }


    public class WE
    {
        public QueryParamList paramlist;

        internal string _cond = "";

        public string scope = "";

        public WE(string scope = "")
        {
            this.scope = scope;
        }

        public static string getOpCode(Op op)
        {
            switch (op)
            {
                case Op.Eq: return "=";
                case Op.NotEq: return "<>";
                case Op.Gt: return ">";
                case Op.Lt: return "<";
                case Op.GtEq: return ">=";
                case Op.LtEq: return "<=";
                case Op.LIKE: return " LIKE ";
            }
            return "";
        }

        public WE append(string op, string t, List<SqlParameter> p)
        {
            if (this._cond.Trim().Length > 0)
            {
                this._cond += " " + op + " ";
            }
            this._cond += " " + t + " ";
            if (p != null)
            {
                if (this.paramlist == null)
                {
                    this.paramlist = new QueryParamList();
                }
                this.paramlist.items.AddRange(p);
            }
            return this;
        }

        public WE and(string t, object v, Op op=Op.Eq, object v2 = null)
        {
            if (op == Op.Between)
            {
                string p1 = getParamName(t + "A"), p2 = getParamName(t + "B");
                string c = string.Format(" ({0} >= @{1} AND {0} <= @{2}) ", t, p1, p2);
                return this.append("AND", c, new List<SqlParameter>() { DbTypeInfo.newParameter(p1, v), DbTypeInfo.newParameter(p2, v2) });
            }
            else if (op == Op.BetweenNoEnd)
            {
                string p1 = getParamName(t + "A"), p2 = getParamName(t + "B");
                string c = string.Format(" ({0} >= @{1} AND {0} < @{2}) ", t, p1, p2);
                return this.append("AND", c, new List<SqlParameter>() { DbTypeInfo.newParameter(p1, v), DbTypeInfo.newParameter(p2, v2) });
            } 
            else
            {
                string p1 = getParamName(t);
                string c = string.Format(" {0} {1} @{2} ", t, WE.getOpCode(op), p1);
                return this.append("AND", c, new List<SqlParameter>() { DbTypeInfo.newParameter(p1, v) });
            }
        }

        public WE and(WE c)
        {
            if (string.IsNullOrWhiteSpace(c._cond))
                return this;
            if (c.paramlist != null)
                return this.append("AND", "(" + c._cond + ")", c.paramlist.items);
            return this.append("AND", "(" + c._cond + ")", null);
        }

        public WE and(string t)
        {
            return this.append("AND", t, null);
        }

        public WE and(string t, string n, object v)
        {
            return this.append("AND", t, new List<SqlParameter>() { DbTypeInfo.newParameter(n, v) });
        }

        public WE or(string t, object v, Op op=Op.Eq, object v2 = null)
        {
            if (op == Op.Between)
            {
                string p1 = getParamName(t + "A"), p2 = getParamName(t + "B");
                string c = string.Format(" ({0} >= @{1} AND {0} <= @{2}) ", t, p1, p2);
                return this.append("OR", c, new List<SqlParameter>() { DbTypeInfo.newParameter(p1, v), DbTypeInfo.newParameter(p2, v2) });
            }
            else if (op == Op.BetweenNoEnd)
            {
                string p1 = getParamName(t + "A"), p2 = getParamName(t + "B");
                string c = string.Format(" ({0} >= @{1} AND {0} < @{2}) ", t, p1, p2);
                return this.append("OR", c, new List<SqlParameter>() { DbTypeInfo.newParameter(p1, v), DbTypeInfo.newParameter(p2, v2) });
            }
            else
            {
                string p1 = getParamName(t);
                string c = string.Format(" {0} {1} @{2} ", t, WE.getOpCode(op), p1);
                return this.append("OR", c, new List<SqlParameter>() { DbTypeInfo.newParameter(p1, v) });
            }
        }

        private string getParamName(string p)
        {
            var ret = scope + p.Replace(".", "_");
            var i = 0;
            var t = ret;
            while(paramlist?.items?.Any(x=>x.ParameterName == "@" + t) ?? false)
            {
                i++;
                t = ret + i.ToString(); 
            }
            return t;
        }

        public WE or(WE c)
        {
            if (string.IsNullOrWhiteSpace(c._cond))
                return this;
            if (c.paramlist != null)
                return this.append("OR", "(" + c._cond + ")", c.paramlist.items);
            return this.append("OR", "(" + c._cond + ")", null);
        }

        public WE or(string t)
        {
            return this.append("OR", t, null);
        }

        public WE or(string t, string n, object v)
        {
            return this.append("OR", t, new List<SqlParameter>() { DbTypeInfo.newParameter(n, v) });
        }

        public WE ById(long id)
        {
            return this.and("id", id);
        }

        public WE addParam(string n, object v)
        {
            if (this.paramlist == null)
            {
                this.paramlist = new QueryParamList();
            }

            this.paramlist.items.Add(DbTypeInfo.newParameter(n, v));
            return this;
        }

        public WE addParam(SqlParameter p)
        {
            if (this.paramlist == null)
            {
                this.paramlist = new QueryParamList();
            }

            this.paramlist.items.Add(p);
            return this;
        }

    }


    public class XQ
    {
        internal SqlService srv;
        internal string _row = "";
        internal string _sort = "";
        internal string _group = "";
        internal string _table = "";

        internal Dictionary<string, XQ> _with = null;
        internal DataStore _store;

        public WE where = new WE();

        internal List<string> _fields = new List<string>();
        private List<string> _join;
        private int _top = 0;

        public virtual XQ table(string t)
        {
            this._table = t;

            
            DBTableInfo tinfo = this.srv.getSchema(_table);
            if (this._fields.Count > 0)
            {
                this._fields = this._fields.Intersect<string>(
                    tinfo.fields.ConvertAll<string>(q => q.FieldName).ToList(), StringComparer.OrdinalIgnoreCase
                ).ToList();
            }
            else
            {
                this._fields = tinfo.fields.ConvertAll<string>(q => q.FieldName).ToList();
            }

            //this._fields = this._fields.ConvertAll(dc => "dc." + dc);
            
            return this;
        }

        public XQ With(XQ q, string name = "iz")
        {
            this._fields = this._fields.ConvertAll(z => (z.Contains(".") ? "" : "z.") + z);
            if (this._with == null)
                this._with = new Dictionary<string, XQ>();
            this._with.Add(name, q);
            return this;
        }

        public XQ Join_lo(string join)
        {
            this._fields = this._fields.ConvertAll(z => (z.Contains(".") ? "" : "z.") + z);
            if (this._join == null)
                this._join = new List<string>();
            this._join.Add(join);
            return this;
        }

        public XQ setfields(params string[] f)
        {
            this._fields = f.ToList();
            return this;
        }

        public XQ addFields(params string[] f)
        {
            this._fields.AddRange(f);
            return this;
        }

        public XQ remFields(params string[] f)
        {
            this._fields.RemoveAll(t => f.Contains(t, StringComparer.OrdinalIgnoreCase));
            return this;
        }

        public XQ And(WE where)
        {
            this.where.and(where);
            return this;
        }
        public XQ And(string fn, object v = null, Op op = Op.Eq, object v2 = null)
        {
            if (v == null)
            {
                if (this._store == null || !this._store.hasField(fn))
                {
                    this.where.and(fn);
                    return this;
                }
                v = this._store[fn];
            }

            this.where.and(fn, v, op, v2);
            return this;
        }
        public XQ AddParam(string n, object v)
        {
            this.where.addParam(n, v);
            return this;
        }
        public XQ Or(WE where)
        {
            this.where.or(where);
            return this;
        }
        public XQ Or(string fn, object v = null, Op op = Op.Eq, object v2 = null)
        {
            if (v == null)
            {
                if (this._store == null || !this._store.hasField(fn))
                {
                    this.where.or(fn);
                    return this;
                }
                v = this._store[fn];
            }

            this.where.or(fn, v, op, v2);
            return this;
        }

        public XQ byId(long? id = null)
        {
            string idField = "id";
            if(this._fields.Any(f=>f.Contains("."))){
                idField = "z.id";
            }

            this.And(idField, id ?? (this._store ?? new DataStore()).getAs<long>("id", -1));

            //this.where.ById(id ?? this._store.IdLong);
            return this;
        }
        public XQ sort(string t)
        {
            this._sort = t;
            return this;
        }
        public XQ group(string t)
        {
            this._group = t;
            return this;
        }

        public XQ row(string t)
        {
            this._row = t;
            if (_sort == "")
            {
                _sort = t;
            }
            return this;
        }

        public XQ top(int n)
        {
            this._top = n;
            return this;
        }

        private string CFieldName(string f)
        {
            if (",end,".Contains(string.Format(",{0},", f).ToLower()))
                return string.Format("[{0}]", f);
            return f;
        }

        private string getSelectSql()
        {
            string ret = "";

            if (_row.Trim().Length > 0)
            {
                ret += " ROW_NUMBER() OVER (ORDER BY " + _row + ") AS row ,";
            }

            if (_fields.Count == 0)
            {
                ret += " * ,";
            }
            else
            {
                _fields.ForEach(t => ret += CFieldName(t) + ",");
            }

            ret = ret.TrimEnd(',');

            ret += " FROM " + _table + " z ";

            //if (this._with != null && this._with.Count > 0)
            //{
            //    foreach (var w in this._with.Keys)
            //    {
            //        ret += $", {w} ";
            //    }
            //}

            foreach (var j in this._join ?? new List<string>())
            {
                ret += j + " ";
            }
            ret += this.getWhere();

            if (_group.Trim().Length > 0)
            {
                if (!_group.Trim().StartsWith("GROUP BY", StringComparison.OrdinalIgnoreCase))
                {
                    ret += " GROUP BY ";
                }
                ret += _group;
            }

            if (_sort.Trim().Length > 0)
            {
                if (!_sort.Trim().StartsWith("ORDER BY", StringComparison.OrdinalIgnoreCase))
                {
                    ret += " ORDER BY ";
                }
                ret += _sort;
            }

            if (this.where?.paramlist?.items != null)
            {
                this.where.paramlist.items.RemoveAll(p =>
                {
                    if (new int[] { 1, 5, 6, 9, 13, 17, 21, 25, 26, 27 }.Contains((int)p.DbType))
                    {
                        return false;
                    }
                    var replace = $" {p.Value} ";

                    if (new int[] { 0, 16, 22, 23 }.Contains((int)p.DbType))
                    {
                        replace = $" N'{p.Value}' ";
                    }

                    var pattern = $@"(?i)@\b{p.ParameterName.Replace("@", "")}\b";

                    ret = Regex.Replace(ret, pattern, replace);
                    return true;
                });
            }
            
            return ret;
        }

        public string SelectSql()
        {
            var ret = "SELECT " + (_top > 0 ? " TOP(" + _top + ") " : "") + this.getSelectSql();
            if(this._with != null && this._with.Count > 0)
            {
                var with = "";
                foreach(var w in this._with)
                {
                    if (with != "")
                        with += ",";
                    with += $" {w.Key} AS ({w.Value.SelectSql()}) ";
                    if ((w.Value?.where?.paramlist?.items?.Count ?? 0) > 0)
                    {
                        if (this.where.paramlist == null)
                            this.where.paramlist = new QueryParamList();
                        foreach (var p in w.Value.where.paramlist.items)
                            this.where.paramlist.add(p);
                    }
                }
                ret = $"WITH {with} {ret}";
            }
            return ret;
        }

        public DataList<T> Select<T>(DataAdapter defaultAdapter = null) where T: DataStore, new()
        {
            string ret = this.SelectSql();

            DataList<T> result = srv.ExecuteRead<T>(ret, this.where.paramlist);
            if (defaultAdapter != null)
                result.Adapter = defaultAdapter;
            return result;
        }

        public DataList<DataStore> Select(DataAdapter defaultAdapter = null) {
            return Select<DataStore>(defaultAdapter);
        }

        public T SingleRow<T>(DataAdapter defaultAdapter = null) where T : DataStore, new()
        {
            this.top(1);
            string ret = this.SelectSql();

            var result = srv.SingleRow<T>(ret, this.where.paramlist);
            if (result != null && defaultAdapter != null)
                result.Adapter = defaultAdapter;
            return result;
        }

        public DataStore SingleRow(DataAdapter defaultAdapter = null)
        {
            return (DataStore)this.SingleRow<DataStore>(defaultAdapter);
        }

        public Field SingleValue()
        {
            var x = this.SingleRow<DataStore>();
            if (x != null && x.getAll().Count > 0)
                return x.getAll()[0];
            return null;
        }

        public int Count()
        {
            string ret = "SELECT Count(*) FROM " + _table + " " + this.getWhere();
            return srv.SingleValue(ret, this.where.paramlist).asInt;
        }

        public Field Insert()
        {
            List<string> IdList = this.srv.getSchema(_table).fields.Where(q=>q.isIdentity).Select(z=>z.FieldName).ToList();

            QueryParamList pa = new QueryParamList();
            string ret = "INSERT INTO " + _table;
            string f = "", v = "";

            this._store.getAll().ForEach(t =>
            {
                if (IdList.Contains(t.fn, StringComparer.OrdinalIgnoreCase))
                {
                    return;
                }

                if (t.fieldType != FieldType.DBValue && t.fieldType != FieldType.Data)
                {
                    return;
                }

                if (_fields.Count == 0)
                {
                    // getField schema fields
                    foreach (var z in srv.getSchema(_table).fields)
                    {
                        this._fields.Add(z.FieldName);
                    }
                }

                if (!(_fields.Count > 0 && _fields.Contains(t.fn, StringComparer.OrdinalIgnoreCase)))
                {
                    return;
                }
                //}

                f += " [" + t.fn + "] ,";

                if (t.isNull)
                {
                    v += " null,";
                }
                else if (t.fieldType == FieldType.DBValue)
                {
                    v += " " + t.value.ToString() + ",";
                }
                else
                {
                    v += " @" + t.fn + ",";
                    pa.add(t.fn, t.value);
                }

            });

            f = f.TrimEnd(',');
            v = v.TrimEnd(',');

            if (this.where.paramlist != null)
                pa.items.AddRange(this.where.paramlist.items);

            return srv.SingleValue(ret + " ( " + f + " ) VALUES ( " + v + " );\n SELECT SCOPE_IDENTITY();", pa);
        }

        public void Delete()
        {
            srv.Execute("DELETE FROM " + _table + this.getWhere(), this.where.paramlist);
        }

        public void Update()
        {
            List<string> IdList = this.srv.getSchema(_table).fields.Where(q => q.isIdentity).Select(z => z.FieldName).ToList();

            QueryParamList pa = new QueryParamList();
            string ret = "UPDATE " + _table + " SET ";

            this._store.getAll().ForEach(t =>
            {
                if (IdList.Contains(t.fn, StringComparer.OrdinalIgnoreCase))
                {
                    return;
                }

                if (t.fieldType != FieldType.DBValue && t.fieldType != FieldType.Data)
                {
                    return;
                }

                if (_fields.Count == 0)
                {
                    // getField schema fields
                    foreach (var z in srv.getSchema(_table).fields)
                    {
                        this._fields.Add(z.FieldName);
                    }
                }

                if (!(_fields.Count > 0 && _fields.Contains(t.fn, StringComparer.OrdinalIgnoreCase)))
                {
                    return;
                }

                ret += " [" + t.fn + "] = ";
                if (t.isNull)
                {
                    ret += " null ";
                }
                else if (t.fieldType == FieldType.DBValue)
                {
                    ret += " " + t.value.ToString();
                }
                else
                {
                    ret += " @" + t.fn + " ";
                    pa.add(t.fn, t.value);
                }

                ret += ",";
            });

            ret = ret.TrimEnd(',') + this.getWhere();

            if (this.where.paramlist != null)
                pa.items.AddRange(this.where.paramlist.items);
            srv.Execute(ret, pa);
        }

        public long UpdateOrInsert(long? id = null)
        {
            if (id != null)
            {
                if (id.Value > 0)
                {
                    this.And("id", id.Value).Update();
                    return id.Value;
                }
                else
                {
                    var r = this.Insert();
                    if (this._store != null)
                    {
                        this._store.IdLong = r.asLong;
                    }
                    return r.asLong;
                }
            }
            if (this._store != null)
            {
                if (this._store.IdLong > 0)
                {
                    this.And("id").Update();
                    return this._store.IdLong;
                }
                else
                {
                    var r = this.Insert();
                    this._store.IdLong = r.asLong;
                    return r.asLong;
                }
            }
            throw new InvalidOperationException("can not Update or insert without ID");
        }

        private string getWhere()
        {
            if (this.where._cond.Trim() != "")
            {
                return " WHERE " + this.where._cond;
            }
            return "";
        }

        public XQ(SqlService srv)
        {
            this.srv = srv;
        }
        public XQ(SqlService srv, DataStore store)
            : this(srv)
        {
            this._store = store;
        }

        public XQ(SqlService srv, string table)
            : this(srv)
        {
            this.table(table);
        }

        public XQ(SqlService srv, DataStore store, string table)
            : this(srv, store)
        {
            this.table(table);
        }


        public XQ() : this(SqlService.DefaultInstance) { }
        public XQ(DataStore store) : this(SqlService.DefaultInstance, store) { }

        public XQ(string table) : this(SqlService.DefaultInstance, table) { }

        public XQ(DataStore store, string table) : this(SqlService.DefaultInstance, store, table) { }

        
    }

    public static class DataStoreEx
    {
        public static XQ AsXQ(this DataStore me, SqlService srv)
        {
            return new XQ(srv, me);
        }
        public static XQ AsXQ(this DataStore me, SqlService srv, string table)
        {
            return new XQ(srv, me, table);
        }
        public static XQ AsXQ(this DataStore me)
        {
            return new XQ(me);
        }
        public static XQ AsXQ(this DataStore me, string table)
        {
            return new XQ(me, table);
        }
    }


    public class Query : IQuery
    {
        internal SqlService service = null;

        internal string _table = "";
        internal List<string> _fields = new List<string>();
        private string _where = "";
        private QueryParamList paramlist;

        internal QueryParamList getParamList()
        {
            if (this.paramlist == null)
            {
                this.paramlist = new QueryParamList();
            }
            return this.paramlist;
        }

        public virtual Query table(string t)
        {
            this._table = t;
            if (this.service != null)
            {
                DBTableInfo tinfo = this.service.getSchema(_table);
                if (this._fields.Count > 0)
                {
                    this._fields = this._fields.Intersect<string>(
                        tinfo.fields.ConvertAll<string>(q => q.FieldName).ToList(), StringComparer.OrdinalIgnoreCase
                    ).ToList();
                }
                else
                {
                    this._fields = tinfo.fields.ConvertAll<string>(q => q.FieldName).ToList();
                }
            }

            return this;
        }

        public Query fields(params string[] f)
        {
            this._fields = f.ToList();
            return this;
        }

        public Query addFields(params string[] f)
        {
            this._fields.AddRange(f);
            return this;
        }

        public virtual Query where(string t, List<SqlParameter> p)
        {
            this._where = " WHERE " + t;
            if (p != null)
            {
                if (this.paramlist == null)
                {
                    this.paramlist = new QueryParamList();
                }
                this.paramlist.items.AddRange(p);
            }
            return this;
        }

        public virtual Query where(WE c)
        {
            this._where = " WHERE " + c._cond;
            this.paramlist = c.paramlist;
            return this;
        }

        public virtual Query ById(long id)
        {
            this._where = "id=" + id;
            return this;
        }

        public virtual string getWhere()
        {
            if (_where.Trim().Length > 0)
            {
                if (!_where.Trim().StartsWith("WHERE", StringComparison.OrdinalIgnoreCase))
                {
                    return " WHERE " + _where;
                }
                return _where;
            }
            return " ";
        }

        // raw sql
        protected string sql;
        public virtual string getSql()
        {
            return this.sql;
        }

        //public Query() { }
        public Query(SqlService service, string sql = "")
        {
            this.service = service;
            this.sql = sql;
        }

        public Query Sql(string sq)
        {
            this.sql = sq;
            return this;
        }

        public QueryParamList getParams()
        {
            return this.paramlist;
        }
    }

    public class SelectQuery : Query
    {
        internal string _row = "";
        internal string _sort = "";
        internal Storage.Core.DataAdapter _adapter;

        public SelectQuery(SqlService service = null) : base(service) { }

        public SelectQuery adapter(Storage.Core.DataAdapter a)
        {
            this._adapter = a;
            return this;
        }

        public new SelectQuery table(string t)
        {
            base.table(t);
            return this;
        }

        public new SelectQuery where(string t, List<SqlParameter> p)
        {
            base.where(t, p);
            return this;
        }

        public new SelectQuery where(WE c)
        {
            base.where(c);
            return this;
        }

        public new SelectQuery ById(long id)
        {
            base.ById(id);
            return this;
        }

        public SelectQuery sort(string t)
        {
            this._sort = t;
            return this;
        }

        public SelectQuery row(string t)
        {
            this._row = t;
            return this;
        }

        public new SelectQuery fields(params string[] f)
        {
            this._fields = f.ToList();
            return this;
        }

        public new SelectQuery addFields(params string[] f)
        {
            this._fields.AddRange(f);
            return this;
        }

        public override string getSql()
        {
            string ret = "SELECT ";

            if (_row.Trim().Length > 0)
            {
                ret += " ROW_NUMBER() OVER (ORDER BY " + _row + ") AS row ,";
            }

            if (_fields.Count == 0)
            {
                ret += " * ,";
            }
            else
            {
                _fields.ForEach(t => ret += t + ",");
            }

            ret = ret.TrimEnd(',');

            ret += " FROM " + _table + " " + this.getWhere();


            if (_sort.Trim().Length > 0)
            {
                if (!_sort.Trim().StartsWith(" ORDER BY", StringComparison.OrdinalIgnoreCase))
                {
                    ret += " ORDER BY ";
                }
                ret += _sort;
            }

            return ret;
        }

        public DataList<DataStore> Execute()
        {
            var z = this.service.ExecuteRead(this);
            z.Adapter = this._adapter;
            return z;
        }

        public DataStore SingleRow(IQuery q, QueryParamList p = null)
        {
            var z = this.service.SingleRow(this);
            z.Adapter = this._adapter;
            return z;
        }

        public Field SingleValue(IQuery q, QueryParamList p = null)
        {
            return this.service.SingleValue(this);
        }

    }

    public class UpdateQuery : Query
    {
        public DataStore _store = null;

        public UpdateQuery(SqlService service = null) : base(service) { }

        public new UpdateQuery table(string t)
        {
            base.table(t);
            return this;
        }

        public new UpdateQuery where(string t, List<SqlParameter> p)
        {
            base.where(t, p);
            return this;
        }

        public new UpdateQuery where(WE c)
        {
            base.where(c);
            return this;
        }

        public new UpdateQuery ById(long id)
        {
            base.ById(id);
            return this;
        }

        public new UpdateQuery fields(params string[] f)
        {
            this._fields = f.ToList();
            return this;
        }

        public new UpdateQuery addFields(params string[] f)
        {
            this._fields.AddRange(f);
            return this;
        }

        public UpdateQuery store(DataStore store)
        {
            this._store = store;
            return this;
        }

        public UpdateQuery set(string fn, object value, FieldType type = FieldType.Data)
        {
            if (this._store == null)
            {
                this._store = new DataStore();
            }
            _store.set(fn, value, type);
            return this;
        }

        public override string getSql()
        {
            string ret = "UPDATE " + _table + " SET ";

            _store.getAll().ForEach(t =>
            {

                //if (t.fieldType != FieldType.DBValue && t.fieldType != FieldType.Data)
                //{
                //    return;
                //}

                //if (_fields.Count > 0)
                //{
                if (!_fields.Contains(t.fn, StringComparer.OrdinalIgnoreCase))
                {
                    return;
                }
                //}

                ret += " [" + t.fn + "] = ";
                if (t.isNull)
                {
                    ret += " null";
                }
                else if (t.fieldType == FieldType.DBValue)
                {
                    ret += " " + t.value.ToString();
                }
                else
                {
                    ret += " @" + t.fn + " ";
                    this.getParamList().add(t.fn, t.value);
                }

                ret += ",";
            });

            ret = ret.TrimEnd(',') + this.getWhere();

            return ret;
        }

        public void Execute()
        {
            this.service.Execute(this);
        }
    }

    public class InsertQuery : Query
    {
        public DataStore _store = null;

        public InsertQuery(SqlService service = null) : base(service) { }

        public new InsertQuery table(string t)
        {
            base.table(t);
            return this;
        }

        public InsertQuery store(DataStore store)
        {
            this._store = store;
            return this;
        }

        public new InsertQuery fields(params string[] f)
        {
            this._fields = f.ToList();
            return this;
        }

        public new InsertQuery addFields(params string[] f)
        {
            this._fields.AddRange(f);
            return this;
        }

        public override string getSql()
        {
            string ret = "INSERT INTO " + _table;
            string f = "", v = "";

            _store.getAll().ForEach(t =>
            {
                //if (t.fieldType != FieldType.DBValue && t.fieldType != FieldType.Data)
                //{
                //    return;
                //}

                //if (_fields.Count > 0)
                //{
                if (!_fields.Contains(t.fn, StringComparer.OrdinalIgnoreCase))
                {
                    return;
                }
                //}

                f += " [" + t.fn + "] ,";

                if (t.isNull)
                {
                    v += " null,";
                }
                else if (t.fieldType == FieldType.DBValue)
                {
                    v += " " + t.value.ToString() + ",";
                }
                else
                {
                    v += " @" + t.fn + ",";
                    this.getParamList().add(t.fn, t.value);
                }

            });

            f = f.TrimEnd(',');
            v = v.TrimEnd(',');

            return ret + " ( " + f + " ) VALUES ( " + v + " );\n SELECT SCOPE_IDENTITY();";
        }

        public Field Execute()
        {
            return this.service.SingleValue(this);
        }

    }

    public class DeleteQuery : Query
    {
        public DeleteQuery(SqlService service = null) : base(service) { }

        public new DeleteQuery table(string t)
        {
            base.table(t);
            return this;
        }

        public new DeleteQuery where(string t, List<SqlParameter> p)
        {
            base.where(t, p);
            return this;
        }

        public new DeleteQuery where(WE c)
        {
            base.where(c);
            return this;
        }

        public new DeleteQuery ById(long id)
        {
            base.ById(id);
            return this;
        }

        public override string getSql()
        {
            return "DELETE FROM " + _table + this.getWhere();
        }

        public void Execute()
        {
            this.service.Execute(this);
        }

    }


    public struct DbTypeInfo
    {
        //public static SqlParameter newParameter(string pName, object value, SqlDbType type
        //    , bool? condition = null, Object falseValue = null, ParameterDirection dir = ParameterDirection.Input)
        //{
        //    SqlParameter result = new SqlParameter();
        //    result.ParameterName = pName;
        //    result.SqlDbType = type;
        //    result.Direction = dir;
        //    result.Value = value;

        //    if (condition.HasValue && condition.Value == false)
        //    {
        //        result.Value = falseValue;
        //    }


        //    return result;
        //}

        public static SqlParameter newParameter(string pName, object value)
        {
            SqlParameter result = new SqlParameter();
            result.ParameterName = (!pName.StartsWith("@") ? "@" : "") + pName;
            result.SqlDbType = getSqlType(value);
            result.Direction = ParameterDirection.Input;
            result.Value = value;
            return result;
        }

        public static SqlDbType getSqlType(object value)
        {
            if (value == null)
                return SqlDbType.Variant;
            Type t = value.GetType();
            foreach (DbTypeInfo ti in DbTypeInfo.dbTypes)
            {
                if (ti.Type == t)
                {
                    return ti.SqlDbType;
                }
            }
            return SqlDbType.Variant;
        }

        public Type Type;
        public DbType DbType;
        public SqlDbType SqlDbType;
        public DbTypeInfo(Type type, DbType dbType, SqlDbType sqlDbType)
        {
            this.Type = type;
            this.DbType = dbType;
            this.SqlDbType = sqlDbType;
        }

        private static List<DbTypeInfo> dbTypes = new List<DbTypeInfo>();

        static DbTypeInfo()
        {
            dbTypes.Add(new DbTypeInfo(typeof(bool), DbType.Boolean, SqlDbType.Bit));
            dbTypes.Add(new DbTypeInfo(typeof(byte), DbType.Double, SqlDbType.TinyInt));
            dbTypes.Add(new DbTypeInfo(typeof(byte[]), DbType.Binary, SqlDbType.Image));
            dbTypes.Add(new DbTypeInfo(typeof(DateTime), DbType.DateTime, SqlDbType.DateTime));
            dbTypes.Add(new DbTypeInfo(typeof(Decimal), DbType.Decimal, SqlDbType.Decimal));
            dbTypes.Add(new DbTypeInfo(typeof(double), DbType.Double, SqlDbType.Float));
            dbTypes.Add(new DbTypeInfo(typeof(Guid), DbType.Guid, SqlDbType.UniqueIdentifier));
            dbTypes.Add(new DbTypeInfo(typeof(Int16), DbType.Int16, SqlDbType.SmallInt));
            dbTypes.Add(new DbTypeInfo(typeof(Int32), DbType.Int32, SqlDbType.Int));
            dbTypes.Add(new DbTypeInfo(typeof(Int64), DbType.Int64, SqlDbType.BigInt));
            dbTypes.Add(new DbTypeInfo(typeof(object), DbType.Object, SqlDbType.Variant));
            dbTypes.Add(new DbTypeInfo(typeof(string), DbType.String, SqlDbType.NVarChar));
        }

    };

}

/*
 
            System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("fa");
            System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("fa");
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            App.Current.DispatcherUnhandledException += new System.Windows.Threading.DispatcherUnhandledExceptionEventHandler(Current_DispatcherUnhandledException);


            CultureInfo ci = new CultureInfo("fa-IR");

            DateTimeFormatInfo dtfi = ci.DateTimeFormat;
            dtfi.AbbreviatedDayNames = new string[] { "ي", "د", "س", "چ", "پ", "ج", "ش" };
            dtfi.DayNames = new string[] { "يكشنبه", "دوشنبه", "سه شنبه", "چهار شنبه", "پنجشنبه", "جمعه", "شنبه" };
            string[] monthNames = new string[] { "فروردين", "ارديبهشت", "خرداد", "تير", "مرداد", "شهريور", "مهر", "آبان", "آذر", "دي", "بهمن", "اسفند", "" };
            dtfi.AbbreviatedMonthNames = dtfi.MonthNames = dtfi.MonthGenitiveNames = dtfi.AbbreviatedMonthGenitiveNames = monthNames;
            dtfi.AMDesignator = "ق.ظ";
            dtfi.PMDesignator = "ب.ظ";
            dtfi.ShortDatePattern = "yyyy/MM/dd";
            dtfi.FirstDayOfWeek = DayOfWeek.Saturday;

            Calendar cal = new PersianCalendar();

            dtfi.GetType().GetField("calendar", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(dtfi, cal);

            ci.NumberFormat.NumberDecimalSeparator = "/";
            ci.NumberFormat.DigitSubstitution = DigitShapes.NativeNational;
            ci.NumberFormat.NumberNegativePattern = 0;
            ci.NumberFormat.NumberDecimalSeparator = ",";

            System.Threading.Thread.CurrentThread.CurrentCulture = ci;
            System.Threading.Thread.CurrentThread.CurrentUICulture = ci; 
 
 
 */