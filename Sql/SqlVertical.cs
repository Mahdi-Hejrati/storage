using Storage.Core;
using Storage.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Sql
{
    class SqlVertical
    {
        // alow CRUD on master TableName and it's extra in vertical TableName mode
        public string table = "master"; // extras will be masterExtra

        public DataList<DataStore> Read(WE where)
        {
            return null;
        }

        public DataStore Single(long id)
        {
            return null;
        }

        public void Update(DataStore st)
        {

        }

        public void Delete(long id)
        {

        }

        public long Insert(DataStore st)
        {
            return 0;
        }

    }
}
