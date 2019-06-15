# EnergyScience
Cohort 14 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.
This project will attempt to build a model that forecasts thermal load for a building.

## Data instance
Given the Cooling load, heating load, and various other energy consumption sources like Fans, interior equipment/lights, weather conditions like temperature, builing information like type/location and holiday information, can we predict the overall electricity consumption load in a building on a future date


### Prerequisites
```
pip install requirements.txt
Unzip data1.zip under the data directory
```

## Data Ingestion and Wrangling

To collect temperature statistics, execute

```
python temperature_client.py
```

To consume raw data and generate interim datasets

```
python energy_consumption.py
```

## Feature Engineering and Model evaluation
To build an overall model across all locations,
Run the following notebooks

```
Feature_Analysis_and_Engineering.ipynb
model_evaluation.ipynb
```

To build a location specific model (Seattle),
Run the following notebooks

```
Feature_Analysis_and_Engineering-Seattle.ipynb
model_evaluation-Seattle.ipynb
```

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.


## Authors

* **Raj Nishtala**
* **Srinivas Thatakula**
* **Anubha Chopra**


## Acknowledgments
* Georgetown Data Analytics (CCPE)
