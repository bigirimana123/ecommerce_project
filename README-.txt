Distributed Multi-Model E-commerce Analytics

This project demonstrates a full-stack analytics solution using MongoDB, HBase, Apache Spark, and Plotly Dash for analyzing e-commerce data.

Project Structure:

.
├── data/                      # Generated synthetic dataset
│   ├── users.json
│   ├── categories.json
│   ├── products.json
│   ├── sessions.json
│   └── transactions.json
├── output/                   # Output data for dashboard
│   ├── recommendations/
│   ├── cohorts/
│   └── clv/
├── dataset_generator.py      # Generates synthetic e-commerce data
├── mongodb_loader.py         # Loads data into MongoDB
├── mongodb_queries.py        # Aggregation queries using MongoDB
├── hbase_loader.py           # Loads sessions into HBase
├── hbase_query_user_sessions.py  # Retrieves sessions from HBase
├── spark_batch.py            # Spark batch processing and analytics
├── clv_analysis.py           # Cross-system CLV analysis
├── dashboard.py              # Plotly Dash interactive dashboard
└── README.md
