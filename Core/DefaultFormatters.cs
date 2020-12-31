using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Controls;
using System.Windows.Input;

namespace Storage.Core
{
    public static class DefaultFormatterActivator
    {
        public static void Activate()
        {
            DataAdapter.globalFormatters["money"] = new MoneyFormatter();
            DataAdapter.globalFormatters["ActivateInfo"] = new ActivateInfoFormatter();
            DataAdapter.globalFormatters["double"] = new DoubleFormatter();
            DataAdapter.globalFormatters["bvisi"] = new BoolVisibility();
            DataAdapter.globalFormatters["bnotvisi"] = new BoolNotVisibility();
        }
    }

    public class BoolNotVisibility : IDataFormatter
    {
        public void getFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            e.result = Convert.ToBoolean(e.result) ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
        }

        public void setFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            throw new NotImplementedException();
        }
    }


    public class BoolVisibility : IDataFormatter
    {
        public void getFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            e.result = Convert.ToBoolean(e.result) ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        }

        public void setFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            throw new NotImplementedException();
        }
    }

    public class DoubleFormatter: IDataFormatter
    {
        public void getFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            if(e.result == null)
            {
                e.result = "";
            } else
            {
                e.result = string.Format("{0:n}", Convert.ToDouble(e.result));
            }
        }

        public void setFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            var s = e.result as string;
            s = s.Replace(",", "").Replace("،", "").Trim();
            if (string.IsNullOrWhiteSpace(s))
            {
                e.result = null;
            }
            else
            {
                if (double.TryParse(s, out double v))
                {
                    e.result = v;
                }
                else
                {
                    e.result = e.store.getAs<double>(e.field.fn, 0);
                }
            }
        }
    }

    public class MoneyFormatter : IDataFormatter
    {
        public static readonly MoneyFormatter Instance = new MoneyFormatter();

        public void getFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            if(e.result == null)
                e.result = "0";
            else
                e.result = InsertComma(e.result);
        }

        public void setFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            e.result = TryLongMoney((string)e.result);
        }

        public static long TryLongMoney(string s, long d = 0)
        {
            s = s.Replace(",", "").Replace("،", "").Trim();
            long result = d;
            if (long.TryParse(s, out result))
            {
                return result;
            }
            return d;
        }
        
        public string skipComma(string p)
        {
            return p.Replace(",", "").Replace("،", "").Trim();
        }

        public string InsertComma(object value)
        {
            if (value == null || (value as string)=="")
                return "";
            decimal v = Convert.ToDecimal(value);
            return v.ToString("#,#");
        }

        public void CheckOnlyNumber(object sender, TextCompositionEventArgs e)
        {
            int v = 0;
            e.Handled = !(int.TryParse(e.Text, out v) || (e.Text == "."));
        }
        private void Textbox_InsertComma(object sender, TextChangedEventArgs e)
        {
            TextBox control = (TextBox)sender;
            if (!string.IsNullOrEmpty(control.Text.Trim()))
            {
                int d = 0;
                if (int.TryParse(skipComma(control.Text), out d))
                {
                    if (d != 0)
                    {
                        control.Text = InsertComma(d);
                    }
                    else
                    {
                        control.Text = "";
                    }
                }

            }
            control.Select(control.Text.Length, 0);
        }

        public void AssignToEdit(TextBox b)
        {
            b.TextChanged += this.Textbox_InsertComma;
            b.PreviewTextInput += this.CheckOnlyNumber;
        }
    }

    public class ActivateInfoFormatter : IDataFormatter
    {

        public void getFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            e.result = (bool)e.result ? "فعال" : "غیر فعال";
        }

        public void setFieldFormatValue(DataAdapter sender, DataAdaperEventArgs e)
        {
            e.result = "فعال".Equals((string)e.result, StringComparison.OrdinalIgnoreCase);
        }
    }

    //public class JsonFormatter : IDataFormatter
    //{
    //    public static readonly JsonFormatter Instance = new JsonFormatter();
    //    public object getFieldFormatValue(DataStore dataStore, Field FieldName, string format)
    //    {
    //        // jsonstring to DataStore            
    //        return DataStore.FromJson((string)FieldName._value);
    //    }

    //    public object setFieldFormatValue(DataStore dataStore, Field FieldName, object newValue, string format)
    //    {
    //        // DataStore to jsonstring
    //        if (newValue is DataStore)
    //        {
    //            return ((DataStore)newValue).asJson(format.Contains("/"));
    //        }
    //        return null;
    //    }
    //}

    //public class XmlFormatter : IDataFormatter
    //{
    //    public static readonly XmlFormatter Instance = new XmlFormatter();
    //    public object getFieldFormatValue(DataStore dataStore, Field FieldName, string format)
    //    {
    //        // jsonstring to DataStore            
    //        return DataStore.FromXml((string)FieldName._value);
    //    }

    //    public object setFieldFormatValue(DataStore dataStore, Field FieldName, object newValue, string format)
    //    {
    //        // DataStore to jsonstring
    //        if (newValue is DataStore)
    //        {
    //            string[] fp = format.Split('/');
    //            string fp1 = fp.Length>1 ? fp[1]: "DATA";
    //            bool indent = fp.Length > 1;
    //            return ((DataStore)newValue).asXml(fp1, indent);
    //        }
    //        return null;
    //    }
    //}


}
