# iGrid
Insight 2020 May Data Engineering Project

# Background
Power outages cause economic loss. Due to the rise of WFH, it can be difficult for the old power grid to adjust to the new demand pattern from regular households. To reduce the chance of blackout, utility companies may need a new way to turn off/down unessential appliances when there is regional power demand/supply imbalance. Instead of power grid infrastructure overhaul, the utility companies can exploit the trend of smart appliances and smart plugs, controlling them through home assistants from the cloud when needed.

# Current data schema:
The data produced at the kafka producers has the following format: 
| timestamp | house_id | appliance_id | appliance_label | power |

# Current pipeline
!['o' output](https://imgur.com/g3XjuAo.png)
