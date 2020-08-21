using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using System.Xml.Linq;
using System.ComponentModel;
using System.Windows.Data;
using System.Collections.ObjectModel;
using System.Windows;
using System.Reflection;
using Storage.Sql;
using System.IO;
using System.Dynamic;
using Storage.XML;

namespace Storage.Core
{

    public interface IDataFormatter
    {
        void getFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e);
        void setFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e);
    }

    
    
    
    

    public class DataStore: DynamicObject, INotifyPropertyChanged
    {
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            FieldAccess fn = new FieldAccess(binder.Name);
            if (this.FieldAliases.ContainsKey(fn.fn))
                fn = this.FieldAliases[fn.fn];

            if (this.hasField(fn.fn))
            {
                result = this.getField(fn.fn)[fn.format];
                return true;
            }
            else
            {
                result = null;
                return false;
            }
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            FieldAccess fn = new FieldAccess(binder.Name);
            if (this.FieldAliases.ContainsKey(fn.fn))
                fn = this.FieldAliases[fn.fn];

            if (this.hasField(fn.fn))
            {
                this.getField(fn.fn)[fn.format] = value;
                return true;
            }
            else
            {
                return false;
            }
        }

        public Dictionary<string, FieldAccess> FieldAliases = new Dictionary<string, FieldAccess>() { { "FieldAlias", new FieldAccess("") } };

        protected DataAdapter _adapter = null;
        virtual public DataAdapter Adapter
        {
            get { if (this._adapter == null) this._adapter = new DataAdapter(); return this._adapter; }
            set { this._adapter = value; }
        }

        protected Dictionary<string, Field> fields = new Dictionary<string, Field>(StringComparer.OrdinalIgnoreCase);

        public DataStore()
        {
        }
        public DataStore(Dictionary<string, object> fields)
            : this()
        {
            this.readFrom(fields);
        }

        public DataStore readFrom(Dictionary<string, object> fields)
        {
            foreach (var q in fields)
            {
                if (q.Value is Dictionary<string, object>)
                {
                    this.fields.Add(q.Key, new Field(q.Key, this, new DataStore((Dictionary<string, object>)q.Value)));
                }
                else if (q.Value is List<Dictionary<string, object>>)
                {
                    this.fields.Add(q.Key, new Field(q.Key, this, new DataList<DataStore>((List<Dictionary<string, object>>)q.Value)));
                }
                else
                {
                    this.fields.Add(q.Key, new Field(q.Key, this, q.Value));
                }
                //this.fields.Add(q.Key, new Field(q.Key, this, q.Value));
            }
            return this;
        }

        public DataStore(System.Data.SqlClient.SqlDataReader r):this()
        {
            this.readFrom(r);
        }

        public DataStore readFrom(System.Data.SqlClient.SqlDataReader r)
        {
            for (int i = 0; i < r.FieldCount; i++)
            {
                var v = r.GetValue(i);
                if (v == DBNull.Value)
                    v = null;
                Field f = new Field(r.GetName(i), this, v);
                this.fields[f.fn] = f;
            }
            return this;
        }

        public DataStore(IEnumerable<DataStore> others): this()
        {
            foreach (var o in others)
                this.CopyFrom(o);
        }
		
        public DataStore(DataStore other, params string[] fs) : this()
        {
            if (other != null)
                this.CopyFrom(other, fs);
        }

        public DataStore(params object[] namevalue)
        {
            if (namevalue.Length % 2 != 0)
            {
                throw new ArgumentOutOfRangeException("namevalue", "number of elements is Invalid");
            }

            for (int i = 0; i < namevalue.Length; i += 2)
            {
                this.set((string)namevalue[i], namevalue[i + 1]);
            }
        }

        public Dictionary<string, object> Export(bool forDebug=false)
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            foreach (var q in this.fields)
            {
                if (q.Value.value is DataStore)
                {
                    ret.Add(q.Key, ((DataStore)q.Value.value).Export(forDebug));
                }
                else if (q.Value.value is DataList<DataStore>)
                {
                    ret.Add(q.Key, ((DataList<DataStore>)q.Value.value).Export(forDebug));
                }
                else
                {
                    var v = q.Value.value;
                    if (forDebug)
                    {
                        if(v != null)
                        {
                            v = v.ToString();
                        }
                    }
                    ret.Add(q.Key, v);
                }
            }
            return ret;
        }

        public DataStore setIfNA(params object[] namevalue)
        {
            if (namevalue.Length % 2 != 0)
            {
                throw new ArgumentOutOfRangeException("namevalue", "number of elements is Invalid");
            }

            for (int i = 0; i < namevalue.Length; i += 2)
            {
                this.ifNull((string)namevalue[i], namevalue[i + 1]);
            }
            return this;
        }

        public long IdLong {
            get { return this.hasField("id") ? this.getField("id").asLong : -1; }
            set { this.getField("id").asLong = value; }
        }

        public Field getField(string fn, object def = null)
        {
            if (this.fields.ContainsKey(fn))
            {
                return this.fields[fn];
            }
            var f = this.Adapter.getMissingField(fn, this, def);
            //if (f.fieldType != FieldType.Calculate)
            //{
            this.fields[fn] = f;
            f.store = this;
            //}
            return f;
        }

        public Field getField(string fn, Func<object> def)
        {
            if (this.fields.ContainsKey(fn))
            {
                return this.fields[fn];
            }
            var f = this.Adapter.getMissingField(fn, this, def());
            //if (f.fieldType != FieldType.Calculate)
            //{
            this.fields[fn] = f;
            f.store = this;
            //}
            return f;
        }
        public Field get(string fn, object def = null)
        {
            return this.getField(fn, def);
        }
        public Field get(string fn, Func<object> def)
        {
            return this.getField(fn, def);
        }

        public DataStore deleteField(string fn)
        {
            this.fields.Remove(fn);
            return this;
        }

        public object this[string fn_format]
        {
            get 
            {
                FieldAccess a = new FieldAccess(fn_format);
                Field f = this.getField(a.fn);
                return f[a.format];
                //return this.Adapter.getFieldFormatValue(this, f, a.format);
            }
            set 
            {
                FieldAccess a = new FieldAccess(fn_format);
                Field f = this.getField(a.fn);
                f[a.format] = value;

                //object Value = this.Adapter.setFieldFormatValue(this, f, value, a.format);
                
                ////if (!Object.Equals(Value, f._value))
                //{
                //    f._value = Value;
                //    this.DoPropertyChanged(f.fn);
                //}
            }
        }

        private DataStoreFields _Fields = null;
        public DataStoreFields Fields
        {
            get
            {
                if (this._Fields == null)
                    this._Fields = new DataStoreFields(this);
                return this._Fields;
            }
        }

        public List<Field> getAll(IEnumerable<string> fns = null)
        {
            if (null == fns)
            {
                return this.fields.Values.ToList();
            } else
            {
                var ret = new List<Field>();
                foreach(var fn in fns)
                {
                    ret.Add(this.get(fn));
                }
                return ret;
            }
        }

        public bool hasField(string fn)
        {
            return this.fields.ContainsKey(fn);
        }

        public bool isNull(string fn)
        {
            return (!this.fields.ContainsKey(fn)) || this.fields[fn].value == null;
        }

        public DataStore ifNull(string fn, object def)
        {
            if (isNull(fn))
                this.set(fn, def);
            return this;
        }

        public DataStore set(string fn, object value)
        {
            this.getField(fn).value = value;
            return this;
        }

        public DataStore setFast(string fn, object value)
        {
            Field f;
            if (this.fields.ContainsKey(fn))
            {
                f = this.fields[fn];
            }
            else
            {
                f = new Field(fn, this, value);
                this.fields[fn] = f;
                f.store = this;
            }
            f._value = value;
            return this;
        }

        public DataStore Clear()
        {
            this.fields.Clear();
            return this;
        }

        public DataStore set(string fn, object value, FieldType ft)
        {
            Field f = this.getField(fn);
            f.value = value;
            f.fieldType = ft;
            return this;
        }

        public DataStore Oprate<T>(string fn, Func<DataStore, T, object> Op, object def = null)
        {
            var f = this.get(fn, def);
            f.value = Op(this, (T)f.value);
            return this;
        }

        public T getAs<T>(string fn, object def = null)
        {
            object v = this.getField(fn, def).value;
            if (v == null)
                v = def;
            Type t = typeof(T);
            if (v == null)
            {
                if (t.IsValueType)
                {
                    return (T)Activator.CreateInstance(t);
                }
            }
            try
            {
                return (T)Convert.ChangeType(v, t);
            }
            catch { }
            return (T)def;
        }

        public DataStore CopyFrom(DataStore other, params string[] fs)
        {
            if (other == null)
                return this;
            if (fs != null && fs.Length > 0)
            {
                foreach (var fn in fs)
                {
                    Field f = other.getField(fn);
                    Field f_t = this.getField(fn);
                    f_t.value = f.value;
                    f_t.fieldType = f.fieldType;
                }
            }
            else
            {
                foreach (var f in other.getAll())
                {
                    Field f_t = this.getField(f.fn);
                    f_t.value = f.value;
                    f_t.fieldType = f.fieldType;
                }
            }
            this.DoPropertyChanged("");
            return this;
        }

        public static DataStore FromJson(string json)
        {
            return new DataStore(JsonConvert.DeserializeObject<Dictionary<string, object>>(json));
        }

        public static DataStore FromXml(string xml)
        {
            DataStore ret = new DataStore();
            XElement node = XElement.Parse(xml);
            foreach (var n in node.Attributes())
            {
                ret.fields[n.Name.ToString()] = new Field(n.Name.ToString(), ret, n.Value);
            }
            return ret;
        }

        public string asJson(bool formatted = false)
        {
            return JsonConvert.SerializeObject(this.Export(), formatted ? Formatting.Indented : Formatting.None);
        }

        public string asXml(string tag, bool formatted = false)
        {
            XElement ret = new XElement(tag);
            this.getAll().ForEach(f => ret.SetAttributeValue(f.fn, f.value));
            return ret.ToString(formatted?SaveOptions.None:SaveOptions.DisableFormatting);
        }

        public virtual void DoPropertyChanged(string fn)
        {
            CallPropertyChanged(fn);
            CallPropertyChanged(Binding.IndexerName);
            CallPropertyChanged("Fields");
            this.FieldAliases.Where(kv => kv.Value.fn.Equals(fn, StringComparison.OrdinalIgnoreCase))
                    .ForEach(kv => CallPropertyChanged(kv.Key));

            //PropertyChangedEventHandler p = this.PropertyChanged;
            //if (p != null)
            //{
            //    p(this, new PropertyChangedEventArgs(Binding.IndexerName));
            //    p(this, new PropertyChangedEventArgs("Fields"));

            //    this.FieldAliases.Where(kv => kv.Value.fn.Equals(fn, StringComparison.OrdinalIgnoreCase))
            //        .ForEach(kv => p(this, new PropertyChangedEventArgs(kv.Key)));
            //}
            this.Adapter.RaiseOnPropertyChanged(this, fn);
        }


        public void CallPropertyChanged(string fn)
        {
            PropertyChangedEventArgs propertyChangedEventArgs = new PropertyChangedEventArgs(fn);
            PropertyChangedEventHandler p = this.PropertyChanged;
            p?.Invoke(this, propertyChangedEventArgs);
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public T MapTo<T>(T dest) 
        {
            foreach (PropertyInfo p in dest.GetType().GetProperties())
            {
                if (this.hasField(p.Name))
                {
                    p.SetValue(dest, this.getField(p.Name).value, null);
                }
            }
            return dest;
        }
        
        public DataStore CopyObjectProps(object src)
        {
            foreach (PropertyInfo p in src.GetType().GetProperties())
            {
                var value = p.GetValue(src);

                if (value == null || p.DeclaringType.IsAutoLayout)
                    this.set(p.Name, value);
                else
                {
                    this.set(p.Name, new DataStore().CopyObjectProps(value));
                }
            }
            return this;
        }

        public string Debug
        {
            get
            {
                return DataStore2xml.Convert2xml(this, "");
            }
        }

        internal void CalculateFields()
        {
            foreach (var f in this.fields.Values)
            {
                if (f is CalculatedField)
                {
                    ((CalculatedField)f).Calculate();
                }
            }
        }

        public List<T> getList<T>(string fn)
        {
            Field f = this.getField(fn, () => new List<T>());
            return (List<T>)f._value;
        }
    }

    public class DataStoreFields
    {
        private DataStore dataStore;
        internal DataStoreFields(DataStore ds)
        {
            this.dataStore = ds;
        }

        public Field this[string fn]
        {
            get { return this.dataStore.getField(fn); }
        }
    }
    
    public struct FieldAccess
    {
        public string fn;
        public string format;

        public FieldAccess(string fn)
        {
            string[] p = fn.Split(':');
            this.fn = p[0];
            this.format = "";
            if (p.Length > 1)
            {
                this.format = p[1];
            }
        }
    }

    public static class ObservableCollectionEx
    {
        public static void ForEach<T>(this IEnumerable<T> me, Action<T> action)
        {
            foreach (var cur in me)
            {
                action(cur);
            }
        }

        public static IEnumerable<O> ConvertAll<T, O>(this IEnumerable<T> me, Func<T, O> action)
        {
            List<O> result = new List<O>();
            foreach (var cur in me)
            {
                result.Add(action(cur));
            }
            return result;
        }
    }

    public static class DataStoreSaver
    {
        public static XElement SaveAsXML(DataStore me, string name)
        {
            XElement x = new XElement(name);
            me.getAll().ForEach(f =>
            {
                var v = f.value;
                if (v == null)
                {
                    x.SetAttributeValue(f.fn, "");
                }
                else if (v is DataStore)
                {
                    x.Add(DataStoreSaver.SaveAsXML(v as DataStore, f.fn));
                }
                else if (v is DataList<DataStore>)
                {
                    x.Add(DataStoreSaver.SaveAsXML(v as DataList<DataStore>, f.fn));
                }
                else if (v is ValueType)
                {
                    x.SetAttributeValue(f.fn, v);
                }
                else
                {
                    x.SetAttributeValue(f.fn, v.GetType().ToString());
                }
            });
            return x;

        }

        private static XElement SaveAsXML(DataList<DataStore> dataList, string name)
        {
            XElement ret = new XElement(name + "s");
            dataList.ForEach(t=>ret.Add(DataStoreSaver.SaveAsXML(t, name)));
            return ret;
        }
    }

    public struct DataStoreComparerFields
    {
        public string Field { get; set; }
        public object Def { get; set; }
        public bool Asc { get; set; }
    }
    public class DataStoreComparer : IComparer<DataStore>
    {
        private List<DataStoreComparerFields> fields;

        public DataStoreComparer(string field, object def = null, bool asc = true)
        {
            this.fields = new List<DataStoreComparerFields>() {
                new DataStoreComparerFields() { Field = field, Asc = asc, Def = def }
            };
        }

        public DataStoreComparer(IEnumerable<DataStoreComparerFields> fields)
        {
            this.fields = fields.ToList();
        }

        public DataStoreComparer addSort(string field, object def = null, bool asc = true)
        {
            this.fields.Add(new DataStoreComparerFields() { Field = field, Asc = asc, Def = def });
            return this;
        }

        public int Compare(DataStore x, DataStore y)
        {
            foreach(var s in fields)
            {
                var vx = x.get(s.Field, s.Def).value as IComparable;
                var vy = y.get(s.Field, s.Def).value as IComparable;

                if (vx==null || vy == null)
                {
                    if(vx != null)
                        return s.Asc ? 1 : -1;
                    if (vy != null)
                        return s.Asc ? -1 : 1;
                    continue;
                }

                var c = vx.CompareTo(vy);
                if (c != 0)
                    return c;
            }
            return 0;
        }
    }
}
