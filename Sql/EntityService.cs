using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Storage.Sql;
using Storage.Core;

namespace Storage.EntityModel
{
    public class EntityService
    {
        public static DBTableInfo EntitySchema = null;

        internal SqlService storage;

        public EntityService(SqlService storage)
        {
            this.storage = storage;

            if (EntitySchema == null)
            {
                EntitySchema = storage.getSchema("Entity");
            }

        }

        public DataStore read(long id, string type, bool full = false)
        {
            string query = "SELECT * FROM " + this.getEntityTable(type, full) + " WHERE gid=" + id;
            return storage.SingleRow(query);
        }

        //public abstract Entity create(string type);

        //public abstract List<Entity> byType(string type, bool full = false);
        //public abstract List<Entity> query(WE where, bool full = false);

        public void Connect(long master, long detail, string relation)
        {

        }

        public bool isEntityExpandable(string type)
        {
            return this.storage.getSchema(type).fields.Count > 0;
        }
        public string getEntityTable(string type, bool full = false, string alias = "E")
        {
            if (!(full && this.isEntityExpandable(type)))
            {
                return " Entity " + alias;
            }

            return "(SELECT EN.*, DD.* FROM Entity EN LEFT OUTER JOIN " + type + " DD ON EN.gid = DD.Id) " + alias;
        }

    }

    //public abstract class Entity: DataStore
    //{
    //    internal EntityService entityService = null;
    //    public long Id
    //    {
    //        getField { return this.getField("gid").ifNull(-1).asLong; }
    //        protected set { this.getField("gid").asLong = value; }
    //    }

    //    public string type
    //    {
    //        getField { return this.getField("type").asString; }
    //        protected set { this.getField("type").asString = value; }
    //    }

    //    public bool isActive
    //    {
    //        getField { return this.getField("isActive").asBool; }
    //        set { this.getField("isActive").asBool = value; }
    //    }

    //    public Entity Parent
    //    {
    //        getField { return this["parentId"].isNull ? null : this.entityService.read(this["parentId"].asLong); }
    //        set { this["parentId"].value = value == null? null : (object)value.Id; }
    //    }

    //    public Entity Container
    //    {
    //        getField { return this["containerId"].isNull ? null : this.entityService.read(this["containerId"].asLong); }
    //        set { this["containerId"].value = value == null ? null : (object)value.Id; }
    //    }

    //    public Entity Owner
    //    {
    //        getField { return this["ownerId"].isNull ? null : this.entityService.read(this["ownerId"].asLong); }
    //        set { this["ownerId"].value = value == null ? null : (object)value.Id; }
    //    }

    //    public Entity Creator
    //    {
    //        getField { return this["creatorId"].isNull ? null : this.entityService.read(this["creatorId"].asLong); }
    //        set { this["creatorId"].value = value == null ? null : (object)value.Id; }
    //    }

    //    public Entity refresh(bool full = true)
    //    {
    //        string query = "SELECT * FROM " + this.entityService.getEntityTable(this.type, full) +  " WHERE gid=" + this.Id;

    //        this.fields.Clear();

    //        DataStore r = entityService.storage.SingleRow(query);
    //        foreach (var t in r.getAll())
    //        {
    //            this.fields[t.fn] = t;
    //            this.fields[t.fn].store = this;
    //        }

    //        return this;
    //    }

    //    public Entity Full()
    //    {
    //        DataStore r = entityService.storage.SingleRow("SELECT * FROM " + this.type + " WHERE marizId=" + this.Id);
    //        if (r != null)
    //        {
    //            foreach (var t in r.getAll())
    //            {
    //                this.fields[t.fn] = t;
    //                this.fields[t.fn].store = this;
    //            }
    //        }
    //        return this;
    //    }

    //    public long Save()
    //    {
    //        XQ q = this.AsXQ(this.entityService.storage);

    //        if (this.Id > 0)
    //        {
    //            q.TableName("Entity").Update();
    //            if (entityService.isEntityExpandable(this.type))
    //            {
    //                q.TableName(this.type).Update();
    //            }
    //        }
    //        else
    //        {
    //            long gid = q.TableName("Entity").Insert().asLong;
    //            this.setD("marizId", gid).setD("gid", gid);
    //            if (entityService.isEntityExpandable(this.type))
    //            {
    //                q.TableName(this.type).Insert();
    //            }
    //        }

    //        return this.refresh().Id;
    //    }

    //    public List<Entity> getDetails(string relation, bool full = false)
    //    {
    //        return null;
    //    }

    //    public List<Entity> getMasters(string relation, bool full = false)
    //    {
    //        return null;
    //    }

    //    public void addDetail(string relation, Entity E)
    //    {
    //        entityService.Connect(this.Id, E.Id, relation);
    //    }

    //    public void addDetail(string relation, long EId)
    //    {
    //        entityService.Connect(this.Id, EId, relation);
    //    }

    //    public void addMaster(string relation, Entity E)
    //    {
    //        entityService.Connect(E.Id, this.Id, relation);
    //    }

    //    public void addMaster(string relation, long EId)
    //    {
    //        entityService.Connect(EId, this.Id, relation);
    //    }

    //    internal Entity(string type)
    //    {
    //        this.Id = -1;
    //        this.type = type;
    //    }

        
    //}
}
