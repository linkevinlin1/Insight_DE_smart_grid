# iGrid
Making power grid smarter. An Insight 2020 May Data Engineering Project by Kevin Yi-Wei Lin.

# Table of Contents
1. [Background](README.md#Background)
2. [Demo](README.md#Demo)
3. [Slide Deck](README.md#Slide-Deck)
4. [Data Pipeline](README.md#Data-Pipeline)
5. [Repository Structure](README.md#Repository-Structure)
6. [Instructions](README.md#Instructions)

# Background
Power outages cause economic loss. Due to the rise of WFH, it can be difficult for the old power grid to adjust to the new demand pattern from regular households. To reduce the chance of blackout, utility companies may need a new way to turn off/down unessential appliances when there is regional power demand/supply imbalance. Instead of power grid infrastructure overhaul, the utility companies can exploit the trend of smart appliances and smart plugs, controlling them through home assistants from the cloud when needed by using long-term and short-term metrics.

# Demo
![iGrid demo](https://i.imgur.com/2F2PrgF.gif)

# Slide Deck
[Link](https://docs.google.com/presentation/d/1HAyDNhsujJUZnfdGOLu7pXEk-ypAYC32fLp_hgiTJZE/edit?usp=sharing)

# Data pipeline
The pipeline was designed to seperate fine-grain analysis (Spark) from coarse-grain analysis. The latter was possible with only a python script because of the roll-up on ingestion and powerful queries powered by Druid. \
![pipeline](https://i.imgur.com/XnhVnkw.png)

# Repository Structure
<pre>
├── batch               batch processing python script with Airfow
├── data                usage of GREEND and REDD data sets
├── database            Druid config files
├── example config      Example configuration file 
├── frontend            Imply Pivot config files
├── ingestion           Kafka producer scripts
└── stream_processing   Spark Structured Streaming script
</pre>

# Instructions
**Data sets**
* The Reference Energy Disaggregation Data Set (**REDD**) [[1]](#1): \
The low frequency data set was used. 
* **GREEND**: Energy Metering Data Set [[2]](#2): \
Version `GREEND_0-2_300615.zip` was used. Please refer to the instruction in `/data`. 

**Cluster setup** \
(I strongly recommend future fellows to utilize AWS managed clusters.)
* Kafka v2.2.1:  AWS MSK three m5.large nodes
* Spark v2.4.5:  AWS EMR v5.30.1 three m5 large nodes (1 master and 2 workers) with bootstrap action script: stream_processing/init_emr.sh
* Druid v0.18.1:  single server "small" using i3.2xlarge
* Kafka producers: four t2.xlarge
* Batch with Airflow: t2.small
* Imply Pivot: t2.medium \
pip3 requirement files are in individual folders

**Create Kafka Topics** \
`powerraw`, `history` and `dutycycle`, where 6 partitions and replication factor 2 were used. 

**Start Kafka Producers** 
1. Change relevant parameters in `config.ini`
2. Place the Python and Bash scripts, `config.ini` and `schema.avsc` under the same directory
3. `./run_GREEND.sh [starting day shift] [ending day shift]` or  \
`./run_REDD.sh [starting day shift] [ending day shift]` \
This will replay the whole data set for each day shift specified in the argument. Do not do more than 20 playbacks on a single machine. 

**Initiate Druid Datasources** \
Change the relevant address and import the specfications into datasources.

**Submit Spark Strutured Streaming Job** 
1. Change relevant parameters in `config.ini` 
2. Place `duty_cycle_avro.py`, `config.ini` and `schema.avsc` under the same directory
3. `spark-submit --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5 duty_cycle_avro.py`

**Start Batch Historcal Processing**
1. Put `druid_batch.py` and `config.ini` under `/home/ubuntu`, or other path specified in the DAG file.
2. Change relevant parameters in `config.ini`
2. Put DAG script in dags folder and run in Airflow

**Dashboard**
1. Connect Pivot to Druid Datasources
2. Import dashboard config file

## References
<a id="1">[1]</a> S. D’Alessandro, A.M. Tonello, A. Monacchi, W. Elmenreich, “GREEND: An Energy Consumption Dataset of Households in Italy and Austria,” Proc. of IEEE SMARTGRIDCOMM 2014, Venice, Italy, November 3-6, 2014. \
<a id="2">[2]</a> J. Zico Kolter and Matthew J. Johnson. REDD: A public data set for energy disaggregation research. In proceedings of the SustKDD workshop on Data Mining Applications in Sustainability, 2011.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
