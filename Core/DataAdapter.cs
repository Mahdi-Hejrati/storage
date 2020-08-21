using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Core
{

    public class DataAdaperEventArgs : EventArgs
    {
        public bool handled { get; set; }
        public DataStore store { get; set; }
        public Field field { get; set; }
        public string format { get; set; }
        public object result { get; set; }
    }

    public class DataAdapterMissingFieldEventArgs : EventArgs
    {
        public bool handled { get; set; }
        public DataStore store { get; set; }
        public string field { get; set; }
        public Field result { get; set; }
        public object def { get; set; }
    }

    public class DataAdapterPropertyChangedEventArgs : EventArgs
    {
        public bool handled { get; set; }
        public DataStore store { get; set; }
        public string field { get; set; }
    }

    public delegate void DataAdapterEvent(object sender, DataAdaperEventArgs e);
    public delegate void DataAdapterMissingEvent(object sender, DataAdapterMissingFieldEventArgs e);
    public delegate void DataAdapterPropertyChanged(object sender, DataAdapterPropertyChangedEventArgs e);

    public class DataAdapter
    {

        internal class FormatterKey
        {
            public string field;
            public string format;

            public FormatterKey(string field, string format)
            {
                this.field = field;
                this.format = format;
            }

            internal FormatterKey setFormat(string v)
            {
                this.format = v;
                return this;
            }

            internal FormatterKey setField(string v)
            {
                this.field = v;
                return this;
            }
        }

        internal class FormatterKeyComparer : IEqualityComparer<FormatterKey>
        {
            public bool Equals(FormatterKey x, FormatterKey y)
            {
                return StringComparer.OrdinalIgnoreCase.Compare(x.field, y.field) == 0
                    && StringComparer.OrdinalIgnoreCase.Compare(x.format, y.format) == 0;
            }
            public int GetHashCode(FormatterKey a)
            {
                return StringComparer.OrdinalIgnoreCase.GetHashCode(a.field)
                    + StringComparer.OrdinalIgnoreCase.GetHashCode(a.format);
            }
        }

        static DataAdapter()
        {
            DefaultFormatterActivator.Activate();
        }

        public static readonly Dictionary<string, IDataFormatter> globalFormatters = new Dictionary<string, IDataFormatter>();

        private Dictionary<FormatterKey, DataFormatter> formatters = new Dictionary<FormatterKey, DataFormatter>(new FormatterKeyComparer());

        internal static IDataFormatter getGlobalFormatter(string format)
        {
            string[] fnp = format.Split('|');
            if (globalFormatters.ContainsKey(fnp[0]))
            {
                return globalFormatters[fnp[0]];
            }
            return null;
        }


        public event DataAdapterEvent OnSetValue;
        public event DataAdapterEvent OnGetValue;
        public event DataAdapterMissingEvent OnMissingField;
        public event DataAdapterPropertyChanged OnPropertyChanged;

        // field:format;p;p;p;p:format;p;p;p:format;p;p;p
        public void registerFormatter(DataFormatter d)
        {
            string[] fns = d.field.Split(new char[] { ',' });
            string[] fos = d.format.Split(new char[] { ',' });
            foreach (var fn in fns)
            {
                foreach (var fo in fos)
                {
                    this.formatters[new FormatterKey(fn, fo)] = d;
                }
            }
        }

        private DataFormatter FindFormatter(string field, string format)
        {
            var key = new FormatterKey(field, format);
            if (this.formatters.ContainsKey(key))
            {
                return this.formatters[key];
            }
            else if (this.formatters.ContainsKey(key.setFormat("")))
            {
                return this.formatters[key];
            }
            else if (this.formatters.ContainsKey(key.setFormat(format).setField("")))
            {
                return this.formatters[key];
            }
            return null;
        }

        private void ForwardFormatter(DataAdaperEventArgs e)
        {
            var f = FindFormatter(e.field.fn, e.format);
            if (f != null && f.Forward != null)
            {
                f.Forward.Invoke(e);
            }
        }

        private void BackwardFormatter(DataAdaperEventArgs e)
        {
            var f = FindFormatter(e.field.fn, e.format);
            if (f != null && f.Backward != null)
            {
                f.Backward.Invoke(e);
            }
        }

        public void PropertyChangedFormatter(DataStore store, string property)
        {
            var f = FindFormatter(property, "");
            if (f != null && f.Changed != null)
            {
                f.Changed.Invoke(store, property);
                //f.Changed2.Invoke(new DataAdaperEventArgs() { store = store, field = store.get(property) });
            }
        }

        private object RaiseSetValue(DataAdaperEventArgs e)
        {
            this.OnSetValue?.Invoke(this, e);
            return e.result;
        }

        private DataAdaperEventArgs RaiseGetValue(DataAdaperEventArgs e)
        {
            this.OnGetValue?.Invoke(this, e);
            return e;
        }

        private DataAdapterMissingFieldEventArgs RaiseMissingField(DataStore st, string field, object def)
        {
            DataAdapterMissingFieldEventArgs e = new DataAdapterMissingFieldEventArgs() { store = st, field = field, def = def, result = null, handled = false };
            this.OnMissingField?.Invoke(this, e);
            return e;
        }

        internal void RaiseOnPropertyChanged(DataStore st, string fn)
        {
            var e = new DataAdapterPropertyChangedEventArgs() { store = st, field = fn };
            this.PropertyChangedFormatter(st, fn);
            this.OnPropertyChanged?.Invoke(this, e);
        }
        public virtual Field getMissingField(string fn, DataStore st, object def)
        {
            DataAdapterMissingFieldEventArgs e = RaiseMissingField(st, fn, def);
            if (e.handled)
            {
                return e.result;
            }
            return new Field(e.field, e.store, e.def);
        }

        public virtual object getFieldFormatValue(DataStore dataStore, Field field, string format)
        {
            var result = RaiseGetValue(new DataAdaperEventArgs() { handled = false, store = dataStore, format = format, field = field, result = field._value });

            ForwardFormatter(result);

            IDataFormatter f = getGlobalFormatter(format);
            if (f != null)
            {
                f.getFieldFormatValue(this, result);
            }

            return result.result;
        }

        public virtual object setFieldFormatValue(DataStore dataStore, Field field, object newValue, string format)
        {

            DataAdaperEventArgs e = new DataAdaperEventArgs() { handled = false, store = dataStore, format = format, field = field, result = newValue };

            IDataFormatter f = getGlobalFormatter(format);
            if (f != null)
            {
                f.setFieldFormatValue(this, e);
            }
            BackwardFormatter(e);
            return RaiseSetValue(e);
        }
    }
}
