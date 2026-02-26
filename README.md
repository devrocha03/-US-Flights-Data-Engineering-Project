# ✈️ Airline Flight Performance Data Platform  
### Plataforma de Engenharia de Dados para Performance de Voos

![PySpark](https://img.shields.io/badge/PySpark-Distributed%20Processing-orange)
![SQL](https://img.shields.io/badge/SQL-Analytics-blue)
![Databricks](https://img.shields.io/badge/Databricks-Cloud%20Platform-red)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-black)
![Architecture](https://img.shields.io/badge/Architecture-Lakehouse-success)

---

## 📌 Executive Summary | Resumo Executivo

**EN:**  
End-to-end Data Engineering project simulating a real airline analytics environment using Lakehouse architecture.

**PT-BR:**  
Projeto completo de Engenharia de Dados simulando o ambiente analítico de uma companhia aérea, utilizando arquitetura Lakehouse.

---

## 🏗️ Architecture Overview | Visão Geral da Arquitetura

Raw Flight Data  
        ↓  
Silver Layer (Cleaning & Feature Engineering)  
        ↓  
Dimensional Modeling (Star Schema)  
        ↓  
Gold Layer (Business Aggregations)  
        ↓  
BI / Executive Dashboard  

---

## 🔹 Silver Layer – Data Engineering  
### Camada Silver – Engenharia de Dados

**EN**
- Schema inference  
- Null handling with `coalesce`  
- Feature engineering  
- Derived metric creation  

**PT-BR**
- Inferência de schema  
- Tratamento de valores nulos  
- Engenharia de atributos  
- Criação de métricas derivadas  

### Engineered Features | Features Criadas
- `Total_Delay`
- `Flight_Status`
- `Day_Period`

---

## ⭐ Dimensional Modeling | Modelagem Dimensional

### Fact Table | Tabela Fato
`fact_flights`

### Dimension Tables | Tabelas Dimensão
- `dim_date`
- `dim_airline`
- `dim_airport`

**EN:** Star Schema optimized for analytical workloads  
**PT-BR:** Modelo estrela otimizado para análises e BI  

---

## 🥇 Gold Layer – Business Metrics  
### Camada Gold – Métricas de Negócio

Table | Tabela: `gold_airline_performance`

Metrics | Métricas:
- Total Flights | Total de Voos
- Average Delay | Atraso Médio
- On-Time Rate | Taxa de Pontualidade
- Monthly Trend | Tendência Mensal

---

## ⚡ Performance Optimization | Otimizações

- Partitioning by FlightDate  
- Delta Lake format  
- OPTIMIZE  
- ZORDER BY Airline, Dep_Airport  

Foco em escalabilidade e eficiência de consulta.

---

## 🛠️ Tech Stack | Tecnologias

- PySpark  
- SQL  
- Databricks  
- Delta Lake  
- Power BI  

---

## 🎯 Professional Objective | Objetivo Profissional

**EN:**  
This project is part of my preparation for my first professional opportunity in Data Engineering.

**PT-BR:**  
Este projeto faz parte da minha preparação para minha primeira oportunidade profissional em Engenharia de Dados.

Actively preparing for my first professional role in Data Engineering, with strong interest in aviation analytics.

**PT-BR:**  
Em preparação ativa para minha primeira oportunidade em Engenharia de Dados, com forte interesse na área de aviação.
