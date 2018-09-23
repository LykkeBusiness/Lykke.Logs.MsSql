using System.Threading.Tasks;

namespace Lykke.Logs.MsSql.Interfaces
{
    public interface ILogRepository
    {
        Task Insert(ILogEntity log);
    }
}
