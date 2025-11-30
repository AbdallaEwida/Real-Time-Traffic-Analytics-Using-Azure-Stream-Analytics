# 🚗 Real-Time Traffic Analytics with Azure Stream Analytics

A real-time data engineering project that simulates, processes, and visualizes live traffic data for Greater Cairo using **Python**, **Azure**, and **Power BI**.

---

## 🌐 Project Overview

This project demonstrates how to build a **complete real-time analytics pipeline** on Microsoft Azure. A custom Python script generates simulated traffic events (speed, vehicle type, timestamp, location) that are streamed to Azure for **real-time analysis** and **interactive visualization**.

**Pipeline Flow:**
Python Generator → Azure Event Hub → Stream Analytics → Azure SQL Database → Power BI

---

## 🧠 Key Highlights

* 🔹 Real-time data streaming and processing
* 🔹 Automated Azure pipeline from ingestion to visualization
* 🔹 Scalable and cloud-based architecture
* 🔹 Live Power BI dashboard for analytics
* 🔹 Designed and developed as part of the *Digital Egypt Pioneers Initiative (DEPI)*

---

## ⚙️ Technologies Used

| Layer                | Tools                  |
| -------------------- | ---------------------- |
| Data Simulation      | Python, Faker          |
| Streaming            | Azure Event Hub        |
| Real-Time Processing | Azure Stream Analytics |
| Storage              | Azure SQL Database     |
| Visualization        | Power BI               |
| Control              | Git & GitHub           |

---

## 🧩 How It Works

1. **Python Simulation:** Generates continuous traffic events with random speeds, directions, and vehicle types.
2. **Azure Event Hub:** Collects the data stream in real time.
3. **Stream Analytics:** Processes events to calculate metrics like average speed and congestion indicators.
4. **Azure SQL Database:** Stores processed insights for querying and reporting.
5. **Power BI Dashboard:** Displays live metrics and traffic summaries.

---

## 🧪 Quick Start

### Prerequisites

* Python 3.8+
* Azure account with Event Hub, Stream Analytics, and SQL Database

### Installation

pip install azure-eventhub faker

### Configuration

Open `traffic_data_simulator.py` and update your Event Hub connection:
EVENTHUB_CONNECTION_STR = "your_connection_string_here"

### Run

python traffic_data_simulator.py

Then monitor your data flow through Azure Event Hub → Stream Analytics → SQL Database → Power BI

---

## 📊 Results & Insights

Once the data is processed, the Power BI dashboard displays real-time metrics such as:

* Average vehicle speed per region
* Total number of events processed per minute
* Vehicle type distribution
* Peak traffic hours
* Traffic density index
  (You can attach a screenshot here of your Power BI dashboard.)

---

## 👥 Team
* Abdalla Nasser Bekhit Abdalla Ewida: Team Leader , Technical Team Lead
* Nour Hassan Hamdy Gamil: Data Simulation & Python Integration
* Zahran Alaa Sayed Mohammed: System Testing & Quality Assurance
* Rawan Khaled Mohamed Mustafa: Data Engineering & SQL Queries 
* Sohaila Ahmed Ali Elgmal: Azure Cloud & Pipeline Architect 
* Menna Muhammad Ali Eissa: Visualization Lead - Power BI 


---
