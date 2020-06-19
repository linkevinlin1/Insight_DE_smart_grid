# iGrid
Insight 2020 May Data Engineering Project

# Background
Power outages cause economic loss. Due to the rise of WFH, it can be difficult for the old power grid to adjust to the new demand pattern from regular households. To reduce the chance of blackout, utility companies may need a new way to turn off/down unessential appliances when there is regional power demand/supply imbalance. Instead of power grid infrastructure overhaul, the utility companies can exploit the trend of smart appliances and smart plugs, controlling them through home assistants from the cloud when needed.

# Demo
![iGrid demo](https://i.imgur.com/2F2PrgF.gif)

# Slide Seck
[Link](https://docs.google.com/presentation/d/1HAyDNhsujJUZnfdGOLu7pXEk-ypAYC32fLp_hgiTJZE/edit?usp=sharing)

# Data pipeline
![pipeline](https://i.imgur.com/XnhVnkw.png)

# Repository Structure
<pre>
├── batch:             batch processing python script with Airfow
├── data:              usage of GREEND and REDD data sets
├── database:          Druid config files
├── frontend:          Imply Pivot config files
├── ingestion:         Kafka producer scripts
└── stream_processing: Spark Structured Streaming script
</pre>

More to come...
