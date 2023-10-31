- # Assignment 3: Building ELT Data Pipelines with Airflow

<div align="center">
  <img width="800" alt="image" src="https://github.com/SavinaySingh/BDE_Assignment3/assets/21008903/ffe350da-b17a-4685-b071-84c598bbeac3">
</div>

### Running the project
Try running the following commands:
- dbt run
- dbt test

## Aim
The goal of this assignment is to build production-ready data pipelines with Airflow. You will work with two different input datasets that will need to be processed and cleaned before loading this insightful information separately into a data warehouse (using ELT pipelines) and a data mart for analytical purposes.

## Introduction to the Datasets

### Airbnb
Airbnb is an online-based marketing company that connects people looking for accommodation (Airbnb guests) to people looking to rent their properties (Airbnb hosts) on a short-term or long-term basis. The rental properties include apartments (dominant), homes, boats, and more. As of 2019, there are 150 million users of Airbnb services in 191 countries, making it a major disruptor of the traditional hospitality industry. We will focus on Sydney for this assignment, using data from May 2020 to April 2021.

### Census
The Census of Population and Housing (Census) is Australia’s largest statistical collection undertaken by the Australian Bureau of Statistics (ABS). It provides a snapshot of Australia, showing how the country has changed over time and allowing for future planning. The Census data helps estimate Australia’s population, which is used to distribute government funds and plan services for the community.

## Data Warehouse Pipeline

1. **Raw Layer**: This layer contains the raw tables and snapshots of dimensions. The snapshots are generated with a strategy based on timestamp, which is a common practice in data warehousing. This allows us to keep historical versions of dimensions over time. 

2. **Staging Layer**: In the staging layer, we perform data cleaning and transformations on the raw and snapshot data. These models need to be materialized as views. 

3. **Warehouse Layer**: The warehouse layer is designed as a star schema with dimensions and fact tables. We directly drop the dimensions' identifiers in the fact table and bring in the descriptions and labels. This layer takes into consideration Slowly Changing Dimensions (SCD) to handle changes in dimension data over time. The models in this layer need to be materialized as tables.

4. **Datamart Layer**: The datamart is where we answer specific analytical questions. Views in this layer need to be materialized as views for performance optimization.

#### Data Mart Views

In the datamart layer, we create the following three views to answer specific questions:

a. **dm_listing_neighbourhood**: This view is designed to provide insights per "listing_neighbourhood" and "month/year." It includes metrics such as active listings rate, price statistics, host information, and more. The view is ordered by "listing_neighbourhood" and "month/year."

b. **dm_property_type**: This view is tailored for analysis per "property_type," "room_type," "accommodates," and "month/year." It contains metrics like active listings rate, price statistics, superhost information, and more. The view is ordered by "property_type," "room_type," "accommodates," and "month/year."

c. **dm_host_neighbourhood**: This view focuses on "host_neighbourhood_lga," which is "host_neighbourhood" transformed to an LGA. It provides insights into the number of distinct hosts, estimated revenue, and estimated revenue per host over "month/year." The view is ordered by "host_neighbourhood_lga" and "month/year."

#### Snapshot Strategy

The strategy for handling snapshots in the raw layer is based on timestamp. This means that we capture historical versions of dimension data. When a dimension changes, a new snapshot is created, allowing us to maintain a historical record of how dimensions evolve over time. The snapshot models in the raw layer are named following the convention: `name_snapshot` (e.g., `property_snapshot`).

By implementing this snapshot strategy, we ensure that we can analyze data as it appeared at different points in time, making it valuable for historical analysis and tracking changes in dimension data.

This architecture and strategy are designed to support comprehensive data analysis and reporting in an efficient and organized manner.

Some definitions:
- Active listings = listing with "has_availability" = "t"
- Active Listing Rate = (total Active listings / total listing) * 100
- Superhost Rate =  (total distinct hosts with "host_is_superhost" = 't' / total distinct hosts) * 100
- Percentage change (month to month) = ((final value - original value) / original value) * 100
- Number of stays (only for active listings) = 30 - availability_30
- Estimated revenue per active listings = for each active listing/period: number of stays * price
- Estimated revenue per host = Total Estimated revenue per active listings/total distinct hosts

### Resources for DBT:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

