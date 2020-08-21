using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Storage.Core;
using System.Xml.Linq;

namespace Storage.XML
{
    public class XMLService
    {
        internal DataAdapter _adapter = null;
        public DataAdapter adapter
        {
            get { if (this._adapter == null) this._adapter = new DataAdapter(); return this._adapter; }
            set
            {
                this._adapter = value;
                foreach (var t in this.tables.Values)
                {
                    t.adapter = value;
                }
            }
        }

        public Dictionary<string, XMLTable> tables = new Dictionary<string, XMLTable>();

        public XMLTable loadTable(string tag, string filename) 
        {
            XMLTable ret = new XMLTable();
            ret.adapter = this.adapter;
            XElement e = XElement.Load(filename);
            foreach (var x in e.Elements(tag))
            {
                ret.AddData(new XmlNode(x));
            }
            this.tables.Add(tag, ret);
            return ret;
        }

        public void saveTable(string table, string filename)
        {
            XMLTable t = this.tables[table];
            XElement x = new XElement(table + "s");
            foreach (var n in t)
            {
                x.Add(n.Save());
            }
            x.Save(filename);
        }

        public XmlNode locate(string id, string table = "*")
        {
            foreach (var t in tables)
            {
                if (table == "*" || table.Equals(t.Key, StringComparison.OrdinalIgnoreCase))
                {
                    XmlNode n = t.Value.SingleOrDefault(x => x.Id.Equals(id, StringComparison.OrdinalIgnoreCase));
                    if (n != null)
                        return n;
                }
            }
            return null;
        }

        public List<XmlNode> locateAll(Func<XmlNode, bool> predicate, string table = "*")
        {
            List<XmlNode> ret = new List<XmlNode>();
            foreach (var t in tables)
            {
                if (table == "*" || table.Equals(t.Key, StringComparison.OrdinalIgnoreCase))
                {
                    foreach (XmlNode x in t.Value)
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

    public class XMLTable: List<XmlNode>
    {
        internal DataAdapter _adapter = null;
        public DataAdapter adapter
        {
            get { if (this._adapter == null) this._adapter = new DataAdapter(); return this._adapter; }
            set { this._adapter = value; this.ForEach(t => t._adapter = value); }
        }

        public XMLTable()
        {

        }

        public XMLTable(string fn)
        {

        }
        
        public void AddData(XmlNode item)
        {
            item.Adapter = this._adapter;
            this.Add(item);
        }
    }


    public class XmlNode: DataStore
    {
        public readonly string TagName;
        public XMLTable children = new XMLTable();

        public string Id
        {
            get { return this.hasField("id") ? this.Fields["id"].asString : ""; }
        }

        public XmlNode(string tag)
        {
            this.TagName = tag;
        }

        public XmlNode(XElement node)
        {
            TagName = node.Name.ToString();
            foreach (var n in node.Attributes())
            {
                this.fields[n.Name.ToString()] = new Field(n.Name.ToString(), this, n.Value);
            }
            if (node.HasElements)
            {
                foreach (var c in node.Elements())
                {
                    this.children.Add(new XmlNode(c));
                }
            }
        }

        public XElement Save()
        {
            XElement ret = new XElement(this.TagName);
            this.getAll().ForEach(f => ret.SetAttributeValue(f.fn, f.value));
            foreach (var c in this.children)
            {
                ret.Add(c.Save());
            }
            return ret;
        }

    }


    //public static class DataStoreExtensions
    //{
    //    public static DataStore AsElement(this DataStore me, string fn)
    //    {
    //        return me;
    //    }

    //    public static DataList<DataStore> Children(this DataStore me, string type = "")
    //    {
    //        return null;
    //    }
    //}

}
