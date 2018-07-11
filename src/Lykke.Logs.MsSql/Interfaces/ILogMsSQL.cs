using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Lykke.Logs.MsSql
{
    public interface ILogMsSql
    {
        Task Insert(ILogObject log);
    }
}
