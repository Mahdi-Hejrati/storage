using Storage.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Xml.Linq;

namespace Storage.XML
{
    public class DataStore2xml
    {
        private static object C2Xml(string fn, object v)
        {
            if (v == null)
            {
                //return new XAttribute($"_NDTN_{fn}", fn);
                return null;
            }
            else if (v is string || v.GetType().IsValueType)
            {
                return new XAttribute(fn, v);
            }
            else if (v is byte[])
            {
                var x = new XElement(fn, Convert.ToBase64String(v as byte[]));
                x.SetAttributeValue("_NDT", "base64");
                return x;
            }
            else if(v is DataStore)
            {
                XElement ret = new XElement(fn);
                foreach (var q in (v as DataStore).getAll())
                {
                    var t = C2Xml(q.fn, q.value);
                    if (t is XAttribute)
                    {
                        ret.SetAttributeValue((t as XAttribute).Name, (t as XAttribute).Value);
                    }
                    else if (t is XElement)
                    {
                        ret.Add(t as XElement);
                    }
                    else if (t is IEnumerable)
                    {
                        foreach (var a in (t as IEnumerable))
                        {
                            ret.Add(a);
                        }
                    }
                }
                return ret;
            }
            else if(v is IEnumerable)
            {
                var l = new List<XElement>();
                foreach(var t in v as IEnumerable)
                {
                    var r = C2Xml(fn, t);
                    if(r is XAttribute)
                    {
                        l.Add(new XElement(fn, (r as XAttribute).Value));
                    } 
                    else if(r is XElement)
                    {
                        l.Add(r as XElement);
                    }
                    else if(r is IEnumerable)
                    {
                        var z = new XElement(fn);
                        foreach(var q in (r as IEnumerable))
                        {
                            z.Add(q);
                        }
                        l.Add(z);
                    }
                }
                return l;
            }
            else
            {
                var x = new XElement(fn, v);
                x.SetAttributeValue("_NDT", v.GetType().Name);
                return x;
            }
        }
        
        public static XElement Convert2XElement(DataStore d)
        {
            return (XElement)C2Xml("root", d);
        }

        public static string Convert2xml(DataStore d, string pass)
        {
            if (!string.IsNullOrWhiteSpace(pass))
            {
                return Encrypt.EncryptString(Convert2XElement(d).ToString(), pass);
            }
            else
            {
                return Convert2XElement(d).ToString();
            }
        }

        public static  void SaveAsXml(string filename, DataStore d, string pass = "")
        {
            File.WriteAllText(filename, Convert2xml(d, pass));
        }

        public static DataStore ReadXmlFile(string filename, string pass = "")
        {
            return ReadXml(File.ReadAllText(filename), pass);
        }

        public static DataStore ReadXml(string xml, string pass = "")
        {
            XElement x;
            if (string.IsNullOrWhiteSpace(pass))
            {
                x = XElement.Parse(xml);
            }
            else
            {
                x = XElement.Parse(Encrypt.DecryptString(xml, pass));
            }

            return ReadDataStore(x);
        }

        private static object ReadXmlValue(XElement x)
        {
            if (x.HasAttributes || x.HasElements)
            {
                var ndt = x.Attribute("_NDT")?.Value ?? "";
                if(ndt == "base64")
                {
                    return Convert.FromBase64String(x.Value);
                }
                return ReadDataStore(x);
            }
            else
                return x.Value;
        }
        
        private static DataStore ReadDataStore(XElement x)
        {
            var ret = new DataStore();
            foreach(var a in x.Attributes())
            {
                var fn = a.Name.ToString();
                //if (fn.StartsWith("_NDTN_"))
                //{
                //    ret.set(a.Value.ToString(), null);
                //    continue;
                //}
                if (fn.StartsWith("_NDT"))
                    continue;
                ret.set(fn, a.Value);
            }

            var elg = x.Elements().GroupBy(z => z.Name.ToString());
            
            foreach(var z in elg)
            {
                if (z.Count() == 1)
                {
                    ret.set(z.Key, ReadXmlValue(z.First()));
                }
                else
                {
                    if(z.Any(t=>t.HasAttributes || t.HasElements))
                    {
                        var l = new List<DataStore>();
                        foreach(var t in z)
                        {
                            l.Add(ReadDataStore(t));
                        }
                        ret.set(z.Key, l);
                    }
                    else
                    {
                        var l = new List<object>();
                        foreach (var t in z)
                        {
                            l.Add(ReadXmlValue(t));
                        }
                        ret.set(z.Key, l);
                    }
                }
            }
            return ret;
        }

        public static bool Test()
        {
            DataStore src = new DataStore();
            src.set("a", 12);
            src.set("b", "mahdi");
            var e = new List<DataStore>();
            e.Add(new DataStore(src));
            e.Add(new DataStore(src));
            e.Add(new DataStore(src));
            src.set("e", e);

            var s = Convert2XElement(src).ToString() ;
            MessageBox.Show(s);

            var dst = ReadXml(s);


            return true;
            
        }
    }
}
