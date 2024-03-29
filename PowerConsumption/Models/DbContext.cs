﻿using Microsoft.EntityFrameworkCore;

namespace PowerConsumption.Models
{
    public class DataModel
    {
    }
    public class Data
    {
        public int Id { get; set; }
        public string Input { get; set; }
        public int Value { get; set; }
    }

    public class ApplicationDbContext : DbContext
    {
        public DbSet<Data> Data { get; set; }
    }

    public class YourDbContext : DbContext
    {
        public YourDbContext(DbContextOptions<YourDbContext> options) : base(options)
        {
        }

        // DbSet properties and other configuration

    }

}
