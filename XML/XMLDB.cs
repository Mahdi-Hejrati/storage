using Storage.Core;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Storage.XML
{
    public class XMLDB
    {
        internal DataAdapter _adapter = null;
        public DataAdapter Adapter
        {
            get { if (this._adapter == null) this._adapter = new DataAdapter(); return this._adapter; }
            set
            {
                this._adapter = value;
                foreach (var t in this.tables.Values)
                {
                    t.Adapter = value;
                }
            }
        }

        public Dictionary<string, DataList> tables = new Dictionary<string, DataList>(StringComparer.OrdinalIgnoreCase);

        public XMLDB loadFolder(string folder)
        {
            if (Directory.Exists(folder))
            {
                foreach(var f in Directory.GetFiles(folder, "*.XML"))
                {
                    string tag = Path.GetFileNameWithoutExtension(f);
                    this.loadTable(tag, f);
                }
            }
            return this;
        }
        public DataList loadTable(string tag, string filename)
        {
            DataList ret = new DataList();
            ret.Adapter = this.Adapter;
            XElement e = XElement.Load(filename);
            foreach (var x in e.Elements(tag))
            {
                ret.Add(DataStoreXmlNode.LoadFromXML(x));
            }
            this.tables.Add(tag, ret);
            return ret;
        }

        public void saveTable(string table, string filename)
        {
            DataList t = this.tables[table];
            XElement x = new XElement(table + "s");
            foreach (var n in t)
            {
                x.Add(n.SaveToXML());
            }
            x.Save(filename);
        }

        public DataStore locate(string id, string table = "*")
        {
            foreach (var t in tables)
            {
                if (table == "*" || table.Equals(t.Key, StringComparison.OrdinalIgnoreCase))
                {
                    DataStore n = t.Value.FirstOrDefault(x => x.xId().Equals(id, StringComparison.OrdinalIgnoreCase));
                    if (n != null)
                        return n;
                }
            }
            return null;
        }

        public DataList locateAll(string id, string table = "*")
        {
            DataList ret = new DataList();
            foreach (var t in tables)
            {
                if (table == "*" || table.Equals(t.Key, StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var x in t.Value)
                    {
                        if (x.xId().Equals(id, StringComparison.OrdinalIgnoreCase))
                        {
                            ret.Add(x);
                        }
                    }
                }
            }
            return ret;
        }

        public DataList locateAll(Func<DataStore, bool> predicate, string table = "*")
        {
            DataList ret = new DataList();
            foreach (var t in tables)
            {
                if (table == "*" || table.Equals(t.Key, StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var x in t.Value)
                    {
                        if (predicate(x))
                        {
                            ret.Add(x);
                        }
                    }
                }
            }
            return ret;
        }
    }

    public static class DataStoreXmlNode
    {
        public static string xId(this DataStore me, string v = null)
        {
            if(v != null)
            {
                me.set("Id", v);
            }
            return me.get("id").asString;
        }

        public static string TagName(this DataStore me, string v = null)
        {
            if (v != null)
            {
                me.set("_TagName", v);
            }
            return me.get("_TagName").asString;
        }

        public static DataList Children(this DataStore me)
        {
            if (me.get("_Children").isNull)
            {
                me.set("_Children", new DataList());
            }
            return (DataList)me.get("_Children").value;
        }

        public static List<DataStore> ChildrenByTag(this DataStore me, string tag)
        {
            return (((DataList)me.get("_Children").value).Where(d => string.Equals(d.TagName(), tag))).ToList();
        }

        public static XElement SaveToXML(this DataStore me)
        {
            XElement ret = new XElement(me.TagName("field_"));
            me.getAll().ForEach((f) => {
                if(!f.fn.StartsWith("_"))
                    ret.SetAttributeValue(f.fn, f.value);
            });
            foreach (var c in me.Children())
            {
                ret.Add(c.SaveToXML());
            }
            return ret;
        }
        public static void SaveToXML(this DataStore me, string filename)
        {
            XElement ret = me.SaveToXML();
            ret.Save(filename);
        }

        public static DataStore LoadFromXML(XElement node)
        {
            DataStore ret = new DataStore();
            ret.TagName(node.Name.ToString());
            foreach (var n in node.Attributes())
            {
                ret.set(n.Name.ToString(), n.Value);
            }

            if (node.HasElements)
            {
                foreach (var c in node.Elements())
                {
                    ret.Children().Add(DataStoreXmlNode.LoadFromXML(c));
                }
            }
            return ret;
        }
        public static DataStore LoadFromXML(string filename)
        {
            if (File.Exists(filename))
            {
                XElement x = XElement.Load(File.OpenText(filename));
                return DataStoreXmlNode.LoadFromXML(x);
            }
            return new DataStore();
        }
    }


    public static class XMLStoreEx
    {
        //public static DataStore MergeWith(this DataStore me, DataStore other, params string[] fs)
        //{
        //    if(fs == null || fs.Length == 0)
        //    {
        //        fs = other.getAll().ConvertAll<string>(f => f.fn).ToArray();
        //    }

        //    foreach(var f in fs)
        //    {
        //        var ff = me.get(f)._value;
        //        if(ff is DataStore)
        //    }
        //}
    }
}
