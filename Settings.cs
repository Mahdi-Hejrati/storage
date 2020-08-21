using Storage.Core;
using Storage.XML;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage
{
    public class Settings: DataStore
    {
        private string filename;
        private string pass;

        public Settings(string filename = "./settings.xml", string pass="")
        {
            this.filename = filename;
            this.pass = pass;
        }

        public Settings Load()
        {
            this.Clear();
            var str = "";
            if (File.Exists(filename))
            {
                str = File.ReadAllText(filename);
            }
            if (!string.IsNullOrWhiteSpace(str))
            {
                var x = DataStore2xml.ReadXml(str, pass);
                this.CopyFrom(x);
            }
            return this;
        }

        public void Save()
        {
            var str = DataStore2xml.Convert2xml(this, pass);
            File.WriteAllText(this.filename, str);
        }

        public List<T> getAsList<T>(string fn)
        {
            return this.get(fn, () => new List<T>()) as List<T>;
        }

    }
}
