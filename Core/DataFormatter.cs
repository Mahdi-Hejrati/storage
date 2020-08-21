using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Core
{
    public class DataFormatter
    {
        public string field = "";
        public string format = "";
        public Action<DataAdaperEventArgs> Forward = null;
        public Action<DataAdaperEventArgs> Backward = null;
        public Action<DataStore, string> Changed = null;
        //public Action<DataAdaperEventArgs> Changed2 = null;
    }

    public class ListDataFormatter : DataFormatter
    {
        public Dictionary<string, DataList<DataStore>> lists = new Dictionary<string, DataList<DataStore>>(StringComparer.OrdinalIgnoreCase);

        public ListDataFormatter()
        {
            base.format = "list";
            base.Forward = (e) =>
            {
                if (this.lists.ContainsKey(e.field.fn) && e.result != null)
                {
                    e.result = this.lists[e.field.fn].SingleOrDefault(x => x.IdLong == Convert.ToInt64(e.result));
                    return;
                }
                e.result = null;
            };

            base.Backward = (e) =>
            {
                e.result = ((DataStore)e.result ?? new DataStore()).getField("id").ifNull(-1).asLong;
            };
        }
        public ListDataFormatter addList(string fn, DataList<DataStore> list)
        {
            this.lists[fn] = list;
            return this;
        }
    }

    public class ItemsControlFormatter : DataFormatter
    {
        public Dictionary<string, System.Windows.Controls.ItemsControl> lists = new Dictionary<string, System.Windows.Controls.ItemsControl>(StringComparer.OrdinalIgnoreCase);

        public ItemsControlFormatter()
        {
            base.format = "select";
            base.Forward = (e) =>
            {
                if (this.lists.ContainsKey(e.field.fn) && e.result != null && this.lists[e.field.fn].ItemsSource != null)
                {
                    //var se = this.lists[e.field.fn] as System.Windows.Controls.Primitives.Selector;
                    //if (se != null)
                    //{
                    //    var sesi = se.SelectedItem as DataStore;
                    //    if (sesi != null && sesi.IdLong == Convert.ToInt64(e.result))
                    //    {
                    //        e.result = sesi;
                    //        return;
                    //    }
                    //}

                    e.result = ((IEnumerable<DataStore>)this.lists[e.field.fn].ItemsSource).SingleOrDefault(x => x.IdLong == Convert.ToInt64(e.result));
                    return;
                }
                e.result = null;
            };

            base.Backward = (e) =>
            {
                e.result = ((DataStore)e.result ?? new DataStore()).getField("id").ifNull(-1).asLong;
            };
        }
        public ItemsControlFormatter addList(string fn, System.Windows.Controls.ItemsControl list)
        {
            this.lists[fn] = list;
            return this;
        }
    }

}
