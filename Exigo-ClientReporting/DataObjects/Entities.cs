using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exigo.ClientReporting
{
    [global::System.AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class SqlSourceEntityMap : Attribute
    {
        public SqlSourceEntityMap(string sqlCommandText)
        {
            this.SqlCommandText = sqlCommandText;
        }
        public string SqlCommandText { get; set; }
        
    }
}
