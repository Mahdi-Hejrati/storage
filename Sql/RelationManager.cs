using Storage.Core;
using Storage.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Sql
{
    public class RelationManager
    {
        private static RelationManager _Instance;
        public static RelationManager Instance
        {
            get { if (_Instance == null) _Instance = new RelationManager() { srv = SqlService.DefaultInstance }; return _Instance; }
        }

        internal SqlService srv;

        public DataList<Relation2> getRelations(string table1, long id1, string relation=null, string table2=null)
        {
            return null;
        }

        public void Connect(Relation2 r)
        {

        }

        public void DisConnect(Relation2 r)
        {

        }
        




    }

    public class Relation2 : DataStore
    {
        public string master
        {
            get { return this.getField("mastertable").asString; }
            set { this.set("mastertable", value); }
        }

        public long masterId
        {
            get { return this.getField("mastertableId").asLong; }
            set { this.set("mastertableId", value); }
        }

        public string slave
        {
            get { return this.getField("slavetable").asString; }
            set { this.set("slavetable", value); }
        }

        public long slaveId
        {
            get { return this.getField("slavetableId").asLong; }
            set { this.set("slavetableId", value); }
        }

        public string relation
        {
            get { return this.getField("relationname").asString; }
            set { this.set("relationname", value); }
        }
    }


}
