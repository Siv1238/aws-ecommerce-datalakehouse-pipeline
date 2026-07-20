Project Name: E-Commerce Retail Analytics Data Lakehouse
Tech Stack: AWS S3, AWS Glue (PySpark), Glue Data Catalog, Athena, SQL

Architecture Flow:
[CSV Files in Local/Notepad] 
       ↓ (Manual Upload)
[AWS S3: raw/orders & raw/products] 
       ↓ (AWS Glue ETL - PySpark Code)
[AWS S3: processed/ (Parquet format)] 
       ↓ (AWS Glue Crawler)
[AWS Glue Data Catalog (ecommerce_db.processed)] 
       ↓ (Amazon Athena)
[SQL Analytics & Dashboards]