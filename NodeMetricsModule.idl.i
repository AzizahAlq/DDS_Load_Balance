module NodeMetricsModule {
    struct nodeMetrics {
        float cpu_load;
        float memory_usage;
        float battery_level;
        float load_avg;
        string node_id;
        float timestamp;  // Uncomment this if you want to track the timestamp
    };

    struct TaskAssignment {
        string task ;
        string node_id;
       // Timestamp for task creation
    };
};
