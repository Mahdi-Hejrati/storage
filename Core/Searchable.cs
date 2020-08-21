using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Threading;

namespace Storage.Core
{
    public class DataSearcher
    {
        private IEnumerable<DataStore> _OrginalList = null;
        public IEnumerable<DataStore> ItemsList { get { return _OrginalList; } }

        public string IdField { get; set; } = "SeqId";
        public string[] SearchFileds { get; set; }
        public string[] SearchCond { get; protected set; }
        
        public IEnumerable<DataStore> Init(IEnumerable<DataStore> orginalList)
        {
            this._OrginalList = orginalList;
            return orginalList;
        }

        public async Task<IEnumerable<DataStore>> Refresh(IEnumerable<DataStore> orginalList)
        {
            this._OrginalList = orginalList;
            return await DoSearch();
        }

        public event Func<IEnumerable<DataStore>, IEnumerable<DataStore>> ApplyFilters;

        public async Task<IEnumerable<DataStore>> DoSearch(string search = null, string field = null)
        {
            var lst = ApplyFilters?.Invoke(_OrginalList) ?? this._OrginalList;
            return await Task.Run(() =>
            {
                if (search != null)
                {
                    this.SearchCond = search
                        .Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Where(s => s.Length > 1)
                        .ToArray();
                }
                if (this.SearchCond == null || this.SearchCond.Count() == 0)
                {
                    return lst;
                }

                long idsr = 0;
                if (this.SearchCond.Count() == 1 && long.TryParse(this.SearchCond[0], out idsr))
                {
                    var x = this._OrginalList.FirstOrDefault(z => z.getAs<long>(this.IdField, -1L) == idsr);
                    if (x != null)
                        return new List<DataStore>() { x };
                }

                string[] fieldslist = this.SearchFileds;
                if (field != null)
                {
                    fieldslist = new string[] { field };
                }
                if (fieldslist == null)
                {
                    fieldslist = new string[] { };
                }

                return lst.Where(p =>
                {
                    var s = string.Join(" ", p.getAll(fieldslist).ConvertAll(f => f.asString ?? ""));
                    return this.SearchCond.All(c => s.Contains(c));
                });
            });
        }

        public DataStore GetById(long v, string field = "ID")
        {
            return this._OrginalList?.FirstOrDefault(x => x.getAs<long>("ID", -1) == v);
        }

        public void ForEach(Action<DataStore> opration)
        {
            this._OrginalList.ForEach(opration);
        }
    }

    public class DataSearcherTimer: DataSearcher
    {
        private DispatcherTimer SearchTimer = new DispatcherTimer() { Interval = TimeSpan.FromMilliseconds(500), IsEnabled = true };
        public int SearchTime_X_500 { get; set; } = 4;

        public DataSearcherTimer()
        {
            this.SearchTimer.Tick += SearchTimer_Tick;
        }
        
        public event Action<IEnumerable<DataStore>> SearchDone;
        
        string timer_Search;
        string timer_field;
        int timer_time = -1;

        public void TimerSearch(string search = null, string field = null)
        {
            timer_time = this.SearchTime_X_500;
            timer_Search = search;
            timer_field = field;
        }

        private async void SearchTimer_Tick(object sender, EventArgs e)
        {
            timer_time -= 1;
            if (timer_time == 0)
            {
                SearchDone?.Invoke(await this.DoSearch(timer_Search, timer_field));
            }
        }
    }
}
