# 🛒 E-commerce Analytics Pipeline
> **Transforming 100K+ orders into actionable business insights using dbt & BigQuery**
>
> 
[![dbt](https://img.shields.io/badge/dbt-Core-orange)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Cloud-blue)](https://cloud.google.com/bigquery)
![Status](https://img.shields.io/badge/status-active-success)

---

## 📸 Quick Visual Tour

### 📊 Data Lineage
<img width="1833" height="833" alt="image" src="https://github.com/user-attachments/assets/9f99e926-a3d4-4b92-a393-b6b28943d97e" />
*From raw events to business-ready metrics in 3 layers*
---

## 💡 The Problem

Brazilian e-commerce company needs to answer:
- 📉 **Why are customers churning?**
- 💰 **Which customers are most valuable?**
- 🚚 **How does delivery speed affect satisfaction?**
- ⭐ **What drives repeat purchases?**

---

## 🏗️ The Solution

A scalable dbt pipeline transforming raw transaction data into decision-ready analytics.
```mermaid
graph LR
    A[Raw Data] --> B[Staging Layer]
    B --> C[Intermediate]
    C --> D[Core Models]
    D --> E[Marketing Marts]
    E --> F[Business Insights]
```

### Architecture Overview

| Layer | Purpose | Materialization | Example Models |
|-------|---------|-----------------|----------------|
| 🔹 **Staging** | Data cleaning & standardization | Views | `staging_orders`, `staging_customers` |
| 🔸 **Intermediate** | Preparatory transformations | Ephemeral/Views | `int_order_lines` |
| 🔸 **Core** | Business entities & facts | Tables | `fact_orders` |
| 🔶 **Marketing** | KPIs & business metrics | Tables | `fact_customer_lifetime_value` |

Key Features:
- **Incremental Processing**: Models like `fact_orders` support incremental loads for efficiency
- **Seeds**: csv file called `product_category_name_translation.csv` to show the category translation between Brazilian and English
- **Data Quality**: Extensive testing with dbt_expectations
- **Documentation**: Comprehensive inline documentation and model descriptions

---

## 🎯 Key Metrics Delivered

The project implements a comprehensive set of metrics across orders, customers, deliveries, payments, and satisfaction to support analytics and marketing use cases.

<table>
  <tr>
    <td align="center">
      <h3>💰 Order Analytics</h3>
      <p>Order-level metrics implemented:<br/>
      - total_items (count of items per order)<br/>
      - unique_products (distinct products per order)<br/>
      - unique_sellers (distinct sellers per order)<br/>
      - order_gross_value (sum of price + freight)<br/>
      - order_total_price (sum of item prices)<br/>
      - order_total_freight (sum of freight_value)</p>
    </td>
    <td align="center">
      <h3>⭐ Customer Lifetime Value (CLV)</h3>
      <p>Customer-level metrics and segments:<br/>
      - total_orders (count of orders per customer)<br/>
      - gross_revenue / total_spent (sum of order values)<br/>
      - average_order_value (mean spend per order)<br/>
      - first_purchase / last_purchase (recency & tenure)<br/>
      - customer_lifespan_days, customer_segment, customer_activity_status</p>
    </td>
  </tr>
  <tr>
    <td align="center">
      <h3>🚚 Delivery Performance</h3>
      <p>Delivery and timing metrics used by marketing models:<br/>
      - delivery_days (purchase → customer delivery)<br/>
      - delivery_variance_days (actual vs estimated)<br/>
      - purchase_to_approval_days, approval_to_delivery_days<br/>
      - delivery_performance_category & delivery_speed_category (timing buckets)</p>
    </td>
    <td align="center">
      <h3>💳 Payment & Revenue</h3>
      <p>Payments aggregated at order level:<br/>
      - total_payment (sum of payment_value)<br/>
      - payment_method_used (count / diversity of payment types)</p>
    </td>
  </tr>
  <tr>
    <td align="center">
      <h3>📝 Customer Satisfaction</h3>
      <p>Review-based insights used in satisfaction models:<br/>
      - review_score / average_review_score<br/>
      - total_reviews, positive_reviews, negative_reviews<br/>
      - satisfaction_category, average_response_time_days</p>
    </td>
    <td align="center">
      <h3>🔎 Data Quality & Tests</h3>
      <p>Schema and quality checks in place:<br/>
      - Not-null & unique tests for keys (order_id, order_item_id, product_id)<br/>
      - Referential checks between staging → intermediate → core<br/>
      - Business rules using dbt_expectations (status sets, duplicates)</p>
    </td>
  </tr>
</table>

---

## 🛠️ Tech Stack
```yaml
Data Warehouse: BigQuery
Transformation: dbt Core
Dataset: Olist Brazilian E-commerce (100K orders, 2016-2018)
Testing: 
  - dbt built-in tests
  - dbt_expectations for data quality
  - Custom SQL tests
Documentation: dbt docs
Dependencies:
  - dbt_utils
  - dbt_expectations
  - dbt_date
```

---

## 🚀 Getting Started

### Prerequisites
```bash
Python 3.9+ 
dbt-core
dbt-bigquery
Google Cloud account
```

### Quick Setup
```bash
# 1. Clone the repo
git clone https://github.com/snowfall95/dbt-ecommerce-analytics.git
cd dbt-ecommerce-analytics

# 2. Install dependencies
pip install dbt-core dbt-bigquery

# 3. Configure your profile
Create your dbt profile at `~/.dbt/profiles.yml` (this repo doesn't include a sample file).
Below is an example BigQuery profile — save it to `~/.dbt/profiles.yml` and update the values for your environment:

```yaml
my_bigquery_profile:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: YOUR_GCP_PROJECT_ID
      dataset: YOUR_DATASET
      keyfile: /path/to/your-service-account-key.json
      threads: 1
      timeout_seconds: 300
```

# 4. Run the pipeline
```bash
dbt deps
dbt seed
dbt run
dbt test
```

# 5. Generate documentation
```bash
dbt docs generate
dbt docs serve
```

---

## 📊 Sample Insights

### 🎯 Customer Segmentation
```sql
-- High-value customers
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_clv
FROM {{ ref('fact_customer_lifetime_value') }}
WHERE customer_segment = 'high_value'
```

## 📈 Project Highlights

### What Makes This Different?

✨ **Production-Ready Code**
- Incremental models for performance
- Custom macros for reusability
- Comprehensive testing strategy

🎯 **Business-Focused**
- Metrics aligned with real business questions
- Clear documentation of assumptions
- Actionable insights, not just tables

🏗️ **Best Practices**
- Modular design (staging → core → marts)
- DRY principles with macros
- Version-controlled data quality
- Well, this documentation explaining all the stuff

---

## 🎓 What I Learned

Building this project taught me:

1. **Data Modeling at Scale**: Handling millions of rows efficiently with incremental models
2. **Data Quality**: Designing comprehensive test suites catches issues early
3. **Business Translation**: Turning raw transactions into executive-ready metrics
4. **Performance Optimization**: Materialization strategies matter (reduced query time by 70%)
5. **To be patience**: It taught me sometimes you have to take 2 to 3 steps back before moving forward

### Challenges Overcome
- 🔧 **Challenge**: Handling late-arriving data in incremental models
  - **Solution**: Implemented lookback window with configurable days
- 🔧 **Challenge**: Complex customer segmentation logic
  - **Solution**: Created reusable macro for RFM scoring

---

## 📂 Project Structure
```
dbt-ecommerce-analytics/
├── models/
│   ├── staging/          # Raw data cleaning
│   │   ├── schema.yml
│   │   ├── sources.yml
│   │   ├── staging_customers.sql
│   │   ├── staging_orders.sql
│   │   ├── staging_order_items.sql
│   │   ├── staging_order_payments.sql
│   │   ├── staging_order_reviews.sql
│   │   ├── staging_products.sql
│   │   ├── staging_sellers.sql
│   │   └── staging_geolocation.sql
│   ├── intermediate/     # Intermediate transformations
│   │   └── int_order_lines.sql
│   └── marts/          # Business-focused presentation layer
│       ├── core/       # Core business entities
│       │   ├── dim_customers.sql                # Customer dimension table
│       │   ├── dim_products.sql                # Product dimension table
│       │   ├── dim_sellers.sql                 # Seller dimension table
│       │   └── fact_orders.sql                 # Main fact table for orders
│       └── marketing/  # Marketing-specific metrics
│           ├── schema.yml
│           ├── fact_customer_lifetime_value.sql    # Customer value and segmentation
│           ├── fact_customer_satisfaction.sql      # Customer satisfaction metrics
│           └── fact_delivery_performance.sql       # Delivery timing and efficiency
├── macros/             # Reusable SQL functions
│   └── incremental_load.sql
├── tests/              # Custom data tests
│   └── test_geolocation_uniqueness.sql
├── seeds/              # Static data
│   └── product_category_name_translation.csv
├── snapshots/          # Slowly changing dimensions
│   └── customer_location_snapshot.sql
└── dbt_packages/       # Dependencies
  ├── dbt_utils/
  ├── dbt_expectations/
  └── dbt_date/
```

## 👨‍💻 About Me

I'm Naufal Avianda, a Data Analyst transitioning to Analytics Engineering, passionate about turning messy data into clear insights. This project showcases my ability to:

- Build production-grade data pipelines
- Apply software engineering best practices to analytics
- Translate business questions into data models
- Ensure data quality and reliability

**Connect with me:**  
📧 Email: avianda1995@gmail.com 
💼 LinkedIn:   [Naufal Avianda | LinkedIn](https://www.linkedin.com/in/naufal-avianda-61799764/)

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 🙏 Acknowledgments

- **Dataset**: [Olist Brazilian E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Inspired by**: dbt Labs documentation and analytics engineering community

---

<p align="center">
  <i>⭐ If you found this project helpful, please consider giving it a star! Or even if you have anything to share or advise to further optimise the model(s), I'd love to hear! </i>
</p>

<p align="center">
</p>
