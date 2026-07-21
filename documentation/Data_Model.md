# Data Model

## Overview

The Supermarket Data Engineering Project follows a structured data model designed to support scalable data processing and business analytics.

The project uses two complementary data modeling approaches:

- **Entity Relationship (ER) Model** for defining relationships between business entities.
- **Medallion Architecture** for organizing data into different processing layers (Bronze, Silver, and Gold).

This combination ensures data consistency, maintainability, and efficient analytical processing.

---

# Entity Relationship Diagram

<p align="center">
    <img src="../images/ER_diagram.png" alt="ER Diagram" width="900"/>
</p>

The ER Diagram illustrates the relationships between the core business entities used in the supermarket retail system.

---

## Business Entities

### Products

Stores product-related information such as:

- Product ID
- Product Name
- Category
- Brand
- Unit Price

Each product can appear in multiple sales transactions, inventory records, and purchase orders.

---

### Stores

Stores information about supermarket branches.

Attributes include:

- Store ID
- Store Name
- Location
- Manager Name

Each store maintains its own inventory and sales transactions.

---

### Inventory

Represents the stock available in each store.

Key attributes include:

- Inventory ID
- Store ID
- Product ID
- Quantity Available
- Last Restocked Date

This entity connects Products and Stores.

---

### Sales Transactions

Contains customer purchase information.

Important attributes include:

- Transaction ID
- Store ID
- Product ID
- Quantity Sold
- Unit Price
- Total Amount
- Transaction Date

This table is used extensively for sales analysis.

---

### Suppliers

Stores supplier information.

Attributes include:

- Supplier ID
- Supplier Name
- Contact Person
- Phone Number
- Email

Suppliers provide products through purchase orders.

---

### Purchase Orders

Represents procurement activities.

Important attributes include:

- Purchase Order ID
- Supplier ID
- Product ID
- Order Date
- Delivery Date
- Quantity Ordered
- Order Status

This entity supports supplier performance analysis.

---

# Entity Relationships

The primary relationships within the data model include:

- One Store → Many Inventory Records
- One Product → Many Inventory Records
- One Store → Many Sales Transactions
- One Product → Many Sales Transactions
- One Supplier → Many Purchase Orders
- One Product → Many Purchase Orders

These relationships ensure data integrity and enable business analysis across multiple functional areas.

---

# Medallion Data Model

<p align="center">
    <img src="../images/supermarket_data_model.png" alt="Medallion Architecture" width="1000"/>
</p>

The project organizes data into multiple processing layers following the Medallion Architecture.

---

## Landing Layer

Purpose:

- Stores raw CSV files ingested from GitHub.
- Acts as the initial storage layer before transformation.

---

## Bronze Layer

Purpose:

- Stores raw data in Delta format.
- Preserves the original source data.
- Serves as the foundation for downstream processing.

---

## Silver Layer

Purpose:

- Cleans and validates data.
- Removes duplicate records.
- Handles missing values.
- Standardizes data types and formats.
- Applies business validation rules.

---

## Gold Layer

Purpose:

- Creates business-ready datasets.
- Performs aggregations.
- Calculates KPIs.
- Supports reporting and analytics.

Gold datasets include:

- Sales Performance
- Monthly Sales Performance
- Product Performance
- Inventory Analysis
- Supplier Performance

---

# Benefits of the Data Model

The implemented data model provides several advantages:

- Clear business entity relationships
- Improved data consistency
- Better data quality
- Scalable cloud architecture
- Simplified analytical reporting
- Efficient query performance
- Easy maintenance and future enhancements

---