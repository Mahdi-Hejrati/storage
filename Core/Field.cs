using Storage.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Core
{
    public class Field
    {
        public FieldType fieldType = FieldType.Data;
        public string fn { get; private set; }
        internal DataStore store;
        public bool isModified { get; set; }

        public Field(string fn, DataStore store, object value, FieldType fieldType = FieldType.Data)
        {
            this.fn = fn;
            this.store = store;
            this.fieldType = fieldType;
            this._value = value;
        }

        internal object _value;

        public object value
        {
            get
            {
                return this.getValue();
            }
            set
            {
                this.setValue(value);
            }
        }

        public virtual object getValue(string format="")
        {
            return this.store.Adapter.getFieldFormatValue(this.store, this, format);
        }

        public virtual void setValue(object v, string format="")
        {
            object val = this.store.Adapter.setFieldFormatValue(this.store, this, v, format);

            if (!Object.Equals(this._value, val))
            {
                this._value = val;
                this.isModified = true;
                //this.store.CalculateFields();
                this.store.DoPropertyChanged(this.fn);
            }
        }

        public object this[string format]
        {
            get { return this.getValue(format); }
            set { this.setValue(value, format); }
        }


        public bool isNull
        {
            get
            {
                return _value == null || _value == DBNull.Value
                    || (fieldType == FieldType.DBValue && ("null".Equals((string)_value, StringComparison.OrdinalIgnoreCase)));
            }
        }

        public Field ifNull(object value)
        {
            if (this.isNull)
                this._value = value;
            return this;
        }

        public int asInt
        {
            get { return Convert.ToInt32(this.value); }
            set { this.value = value; }
        }

        public long asLong
        {
            get { return Convert.ToInt64(this.value); }
            set { this.value = value; }
        }

        public double asDouble
        {
            get { return Convert.ToDouble(this.value); }
            set { this.value = value; }
        }

        public bool asBool
        {
            get { return Convert.ToBoolean(this.value); }
            set { this.value = value; }
        }

        public DateTime asDateTime
        {
            get { return Convert.ToDateTime(this.value); }
            set { this.value = value; }
        }

        public byte[] asBinary
        {
            get { return (byte[])(this.value); }
            set { this.value = value; }
        }

        public string asString
        {
            get { return Convert.ToString(this.value); }
            set { this.value = value; }
        }

        public DataStore asStore
        {
            get { return (DataStore)this.value; }
            set { this.value = value; }
        }

        private Details _detail;
        public Details Detail
        {
            get
            {
                if (this._detail == null)
                {
                    this._detail = new Details(this);
                }
                return this._detail;
            }
        }

        private Lookup _lookup;
        public Lookup lookup
        {
            get { if (_lookup == null) _lookup = new Lookup(this); return _lookup; }
        }

        //public DataStore asJson
        //{
        //    getField
        //    {
        //        return DataStore.FromJson(this.asString);
        //    }
        //    set
        //    {
        //        this.value = value.asJson();
        //    }
        //}

        //public DataList<DataStore> asJsonList
        //{
        //    getField
        //    {
        //        var result = new DataList<DataStore>();
        //        var d = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(this.asString);
        //        foreach (var q in d)
        //        {
        //            result.Add(new DataStore(q));
        //        }
        //        return result;
        //    }
        //    set
        //    {
        //        var d = new List<Dictionary<string, object>>();
        //        foreach (var q in value)
        //        {
        //            d.Add(q.Export());
        //        }
        //        this.value = JsonConvert.SerializeObject(d, UseFormattedJson? Formatting.Indented: Formatting.Empty);
        //    }
        //}

        //public bool UseFormattedJson { getField; set; }
    }

    public class Lookup
    {
        Field field;
        public Lookup(Field field)
        {
            this.field = field;
        }

        private void DecodeParams(string p, out string lst, out string lf)
        {
            var z = p.Split(';');
            lst = z[0];
            lf = "id";
            if (z.Length > 1)
            {
                lf = z[1];
            }
        }

        public DataStore this[string p]
        {
            get
            {
                string lst, lf;
                this.DecodeParams(p, out lst, out lf);
                if (this.field.store.hasField(lst))
                {
                    object v = this.field.value;
                    if (v == null)
                    {
                        return null;
                    }
                    var x = ((DataList<DataStore>)this.field.store.getField(lst).value);
                    if (v.GetType() == typeof(string))
                    {
                        return x.SingleOrDefault(z => string.Equals((string)v, z.getField(lf).asString, StringComparison.OrdinalIgnoreCase));
                    }
                    else
                    {
                        long vv = Convert.ToInt64(v);
                        return x.SingleOrDefault(z => Int64.Equals(vv, z.getField(lf).asLong));
                    }
                }
                return null;
            }

            set
            {
                string lst, lf;
                this.DecodeParams(p, out lst, out lf);
                if (value == null)
                {
                    this.field.value = null;
                }
                else
                {
                    this.field.value = value.getField(lf).value;
                }
            }
        }
    }


    public class CalculatedField : Field
    {
        public Func<DataStore, CalculatedField, object> Calc;

        public CalculatedField(string fn, DataStore store, Func<DataStore, CalculatedField, object> Calc)
            : base(fn, store, null, FieldType.Calculated)
        {
            this.Calc = Calc;
        }

        public void Calculate()
        {
            if (this.Calc != null)
            {
                this._value = Calc(this.store, this);
            }
        }
    }

    public class Details
    {
        internal Field field;

        internal Details(Field field)
        {
            this.field = field;
        }

        public DataList<DataStore> this[string p]
        {
            get
            {
                DetailParams pp = new DetailParams(p);
                var ret = new XQ(pp.dtable).And(pp.dfield, this.field.value).sort(pp.sort).row(pp.row);
                if (pp.selectField.Length > 0)
                {
                    ret.setfields(pp.selectField);
                }
                return ret.Select(this.field.store.Adapter);
            }
        }
    }

    struct DetailParams
    {
        public string dtable;
        public string dfield;
        public string[] selectField;
        public string row;
        public string sort;

        public DetailParams(string str)
        {

            sort = "";
            row = "";


            string[] v = str.Split(';');
            dtable = v[0];
            dfield = v[1];
            if (v.Length > 2 && v[2].Length > 0)
            {
                selectField = v[2].Split('|');
            }
            else
            {
                selectField = new string[0];
            }
            if (v.Length > 3)
            {
                this.row = v[3];
            }
            if (v.Length > 4)
            {
                this.sort = v[4];
            }
            else
            {
                this.sort = this.row;
            }
        }
    }


    public enum FieldType
    {
        Data,
        DBValue,
        Arbitrary,
        Calculated
    }

    public enum FieldDataType
    {
        INT, LONG, BOOL, STRING, DATETIME
    }
}
