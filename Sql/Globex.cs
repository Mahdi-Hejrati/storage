using Storage.Core;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Sql
{
    public static class Globex
    {

        public static string getIdList(List<Uniq> lst, string type = null)
        {
            string ret = "-1";
            if (type == null)
            {
                lst.ForEach(a => ret += "," + a.id);
            }
            else
            {
                lst.Where(a => a.type == type).ForEach(a => ret += "," + a.id);
            }
            return ret;
        }

        public static string getIdList(Uniq master, string relation, string type)
        {
            return getIdList(getRelatedItems(master, relation), type);
        }

        public static void deleteRelations(Uniq master, string relation = null){
            var r = getRelations(master).ToList();
            if (relation != null)
            {
                r.RemoveAll(g => g.relation != relation);
            }
            var relatinsIdList = "-1";
            r.ForEach(g => relatinsIdList += "," + g.IdLong);

            var gl = r.GroupBy(g => g.right.type);
            foreach (var gq in gl)
            {
                var idlist = getIdList(gq.Select(g=>g.right).ToList(), gq.Key);
                SqlService.DefaultInstance.Xq(gq.Key).And("id IN (" + idlist + ")").Delete();
            }
            SqlService.DefaultInstance.Xq("Globex").And("id IN (" + relatinsIdList + ")").Delete();

        }

        public static List<Uniq> getRelatedItems(Uniq master, string relation)
        {
            var q = SqlService.DefaultInstance.Xq("Globex")
                .And("left_type", master.type).And("left_id", master.id)
                .And("relation", relation).Select();
            return q.ConvertAll<DataStore, Uniq>(z => new Uniq(z.getAs<string>("right_type"), z.getAs<long>("right_id")) ).ToList();
        }

        public static DataList<GlobexData> getRelations(Uniq master)
        {
            return SqlService.DefaultInstance.Xq("Globex")
                .And("left_type", master.type).And("left_id", master.id).Select<GlobexData>();
        }

        public static T getSingleData<T>(List<Uniq> rl) where T : DataStore, new()
        {
            if(rl.Count > 0)
                return SqlService.DefaultInstance.Xq(rl[0].type).And("id", rl[0].id).SingleRow<T>();
            return null;
        }

        public static T getData<T>(Uniq rl) where T : DataStore, new()
        {
            return SqlService.DefaultInstance.Xq(rl.type).And("id", rl.id).SingleRow<T>();
        }

        public static DataList<T> getRelatedData<T>(List<Uniq> rl) where T : DataStore, new()
        {
            var g = rl.GroupBy(q => q.type);
            if (g.Count() == 1)
            {
                var a = g.First();
                string idList = "-1";
                foreach (var i in a.ConvertAll<Uniq, string>(u => "," + u.id.ToString()))
                {
                    idList += i;
                }
                return SqlService.DefaultInstance.Xq(a.Key).And("id IN (" + idList + ")").Select<T>();
            }
            else
            {
                DataList<T> ret = new DataList<T>();
                foreach (var a in g)
                {
                    string idList = "-1";
                    foreach (var i in a.ConvertAll<Uniq, string>(u => "," + u.id.ToString()))
                    {
                        idList += i;
                    }
                    foreach (var z in SqlService.DefaultInstance.Xq(a.Key).And("id IN (" + idList + ")").Select<T>())
                    {
                        ret.Add(z);
                    }
                }
                return ret;
            }
        }

        public static long Link(Uniq master, Uniq slave, string relation, string exInfo1 = null, string exInfo2 = null)
        {
            var idf = SqlService.DefaultInstance.Xq("Globex")
                .And("left_type", master.type).And("left_id", master.id)
                .And("right_type", slave.type).And("right_id", slave.id)
                .And("relation", relation).setfields("id").SingleValue();

            if (idf == null || idf.isNull)
            {
                return new DataStore()
                    .set("left_type", master.type).set("left_id", master.id)
                    .set("right_type", slave.type).set("right_id", slave.id)
                    .set("relation", relation).set("exInfo1", exInfo1).set("exInfo2", exInfo2)
                    .AsXQ("Globex").Insert().asLong;
            }
            else
            {
                new DataStore()
                    .set("exInfo1", exInfo1).set("exInfo2", exInfo2)
                    .AsXQ("Globex").byId(idf.asLong).Update();
                return idf.asLong;
            }
        }

        public static void UnLinkAll(Uniq master, string relation)
        {
            SqlService.DefaultInstance.Xq("Globex")
                .And("left_type", master.type).And("left_id", master.id)
                .And("relation", relation).Delete();
        }

        public static void UnLink(Uniq master, Uniq slave, string relation)
        {
            SqlService.DefaultInstance.Xq("Globex")
                .And("left_type", master.type).And("left_id", master.id)
                .And("right_type", slave.type).And("right_id", slave.id)
                .And("relation", relation).Delete();
        }





    }

    public class GlobexRelatedData : ObservableCollection<DataStore>
    {
        private Uniq master;
        private string slave;
        private string relation;

        public GlobexRelatedData(Uniq master, string slave, string relation)
        {
            this.master = master;
            this.slave = slave;
            this.relation = relation;

            this.Reload();
            this.CollectionChanged += this_CollectionChanged;
        }

        private void this_CollectionChanged(object sender, System.Collections.Specialized.NotifyCollectionChangedEventArgs e)
        {
            switch (e.Action)
            {
                case System.Collections.Specialized.NotifyCollectionChangedAction.Add:
                    string.Join(",", e.NewItems);
                    break;
                case System.Collections.Specialized.NotifyCollectionChangedAction.Remove:
                    break;
            }
        }

        private void Reload()
        {

        }
        
    }




    public class Uniq
    {
        public string type;
        public long id;

        public Uniq(string type, long id)
        {
            this.type = type.ToLower();
            this.id = id;
        }
    }

    public class GlobexData : DataStore
    {
        public Uniq left
        {
            get { return new Uniq(this.getAs<string>("left_type"), this.getAs<long>("left_id")); }
            set { this.set("left_type", value.type).set("left_id", value.id); }
        }
        public Uniq right
        {
            get { return new Uniq(this.getAs<string>("right_type"), this.getAs<long>("right_id")); }
            set { this.set("right_type", value.type).set("right_id", value.id); }
        }

        public string relation
        {
            get { return this.getAs<string>("relation"); }
            set { this.set("relation", value); }
        }
    }
}
