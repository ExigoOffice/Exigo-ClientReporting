using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.Data;

namespace Exigo.ClientReporting
{
    public class CustomEntityForPeriodBulkCopyReader<TItem> : SqlBulkCopyReader
    {
        int _periodTy;
        int _periodID;

        IEnumerator<TItem> _enum;
        PropertyInfo[] _props;
        int[] _map;
        public CustomEntityForPeriodBulkCopyReader(int periodTy, int periodID, ICollection<TItem> list, DataTable dt)
        {
            _periodTy = periodTy;
            _periodID = periodID;

            _enum = list.GetEnumerator();
            _props = typeof(TItem).GetProperties();

            _map = new int[_props.Length];

            for (int i = 0; i < _props.Length; i++)
            {
                bool found = false;
                //for (int z = 3; z < dt.Columns.Count; z++)
                for (int z = 2; z < dt.Columns.Count; z++)
                {
                    if (dt.Columns[z].ColumnName == _props[i].Name)
                    {
                        _map[i] = z;
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw new Exception("Could not map " + _props[i].Name);

            }

        }

        public override bool Read()
        {
            return _enum.MoveNext();
        }

        public override int FieldCount
        {
            get { return _props.Length + 2; }
        }

        public override void Close()
        {
            _enum.Dispose();
        }

        public override object GetValue(int i)
        {
            //if (i == 0)
            //    return _companyID;
            //else 
            if (i == 0)
                return _periodTy;
            else if (i == 1)
                return _periodID;
            else
                return _props[_map[i - 2] - 2].GetValue(_enum.Current, null);
        }
    }
}
