using Microsoft.EntityFrameworkCore;

namespace PowerConsumption.Models
{
    public class PeakData
    {
        public int ID { get; set; }
        public int PeakValueColumn { get; set; }
        public DbSet<PeakData> YourModels { get; set; }
    }

}
