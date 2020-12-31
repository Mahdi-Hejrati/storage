using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Core
{
    public class DataList<T> : ObservableCollection<T> where T : DataStore, new()
    {
        internal DataAdapter _adapter = null;
        public DataAdapter Adapter
        {
            get { /*if (this._adapter == null) this._adapter = new DataAdapter();*/ return this._adapter; }
            set
            {
                if (this._adapter != null)
                {
                    this._adapter.OnPropertyChanged -= _adapter_OnPropertyChanged;
                }
                this._adapter = value;
                this.ForEach(t => t.Adapter = value);
                this._adapter.OnPropertyChanged += _adapter_OnPropertyChanged;
            }
        }

        private void _adapter_OnPropertyChanged(object sender, DataAdapterPropertyChangedEventArgs e)
        {
            if (this.Aggregate.hasField(e.field))
            {
                this.UpdateAggregate();
            }
        }

        public void UpdateAggregate()
        {
            var list = this.Aggregate.getAll();
            if (list.Count == 0)
                return;
            var values = new double[list.Count];
            for (var i = 0; i < list.Count; i++)
            {
                values[i] = 0;
            }

            foreach (var d in this)
            {
                for (var i = 0; i < list.Count; i++)
                {
                    values[i] += d.getAs<double>(list[i].fn, 0);
                }
            }

            for (var i = 0; i < list.Count; i++)
            {
                this.Aggregate.set(list[i].fn, values[i]);
            }
        }

        public bool TrackDeletedItems { get; set; } = false;
        public List<DataStore> DeletedItems;

        public DataList()
        {
            this.Adapter = new DataAdapter();
            this.CollectionChanged += this_CollectionChanged;
        }

        public DataList(IEnumerable<T> Items)
        {
            this.addRange(Items);
            this.CollectionChanged += this_CollectionChanged;
        }

        public DataList(List<Dictionary<string, object>> raw)
            : this()
        {
            foreach (var q in raw)
            {
                T t = new T();
                t.readFrom(q);
                this.Add(t);
            }
        }

        public List<Dictionary<string, object>> Export(bool forDebug = false)
        {
            var ret = new List<Dictionary<string, object>>();
            foreach (var d in this)
            {
                ret.Add(d.Export(forDebug));
            }
            return ret;
        }

        public DataStore Flatten(string key, string field = null)
        {
            DataStore result = new DataStore();

            if (field == null)
            {
                foreach (var z in this)
                {
                    result[z.getField(key).asString] = z;
                }
            }
            else
            {
                foreach (var z in this)
                {
                    result[z.getField(key).asString] = z[field];
                }
            }

            return result;
        }

        public DataStore MergeAll()
        {
            DataStore ret = new DataStore();
            ret.set("_mergecount", this.Count);
            foreach (var q in this)
            {
                ret.CopyFrom(q);
            }
            return ret;
        }

        //public DataList<DataStore> Flatten(string keyMaster, string key, string field = null)
        //{
        //    Dictionary<object, List<DataStore>> d = new Dictionary<object, List<DataStore>>();

        //    if (field == null)
        //    {
        //        foreach (var dc in this)
        //        {

        //            if(d.ContainsKey(dc[keyMaster])){

        //            }
        //            d[dc[keyMaster]]   dc.getField(key).asString] = dc;
        //        }
        //    }
        //    else
        //    {
        //        foreach (var dc in this)
        //        {
        //            result[dc.getField(key).asString] = dc[field];
        //        }
        //    }



        //    DataList<DataStore> result = new DataList<DataStore>();




        //    return result;
        //}

        public string Debug
        {
            get
            {
                return StorageUtilis.Fromat_Debug_string(this.ToString());
            }
        }

        private void this_CollectionChanged(object sender, System.Collections.Specialized.NotifyCollectionChangedEventArgs e)
        {
            switch (e.Action)
            {
                case System.Collections.Specialized.NotifyCollectionChangedAction.Add:
                case System.Collections.Specialized.NotifyCollectionChangedAction.Replace:
                    foreach (var t in e.NewItems)
                    {
                        ((DataStore)t).Adapter = this.Adapter;
                    }
                    break;
                case System.Collections.Specialized.NotifyCollectionChangedAction.Remove:
                    if (this.TrackDeletedItems)
                    {
                        if (this.DeletedItems == null)
                        {
                            this.DeletedItems = new List<DataStore>();
                        }
                        foreach (var t in e.OldItems)
                        {
                            this.DeletedItems.Add((DataStore)t);
                        }
                    }
                    break;
            }

            this.UpdateAggregate();
        }


        public int RemoveAll(Func<T, bool> predicate)
        {
            List<T> toremove = this.Where(predicate).ToList();
            foreach (var t in toremove)
            {
                this.Remove(t);
            }
            return toremove.Count;
        }

        public DataList<T> addRange(IEnumerable<T> other)
        {
            foreach (var t in other)
            {
                this.Add(t);
            }
            return this;
        }

        public DataStore Aggregate { get; protected set; } = new DataStore();

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("[");
            foreach(var q in this)
            {
                builder.AppendLine("").Append(q.ToString()).Append(",");
            }
            builder.Replace(",", "", builder.Length - 2, 1);
            builder.AppendLine("").AppendLine("]");
            return builder.ToString();
        }
    }


    public class DataList : DataList<DataStore> { }
}
