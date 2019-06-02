# EnergyScience
Cohort 14 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.
This project will attempt to build a model that forecasts thermal load for a building.

## Data instance
Given the Cooling load, heating load, and various other energy consumption sources like Fans, interior equipment/lights, weather conditions like temperature, builing information like type/location and holiday information, can we predict the overall energy load in a building on a given day

### Data instance - Part 2
Given the Cooling Load, heating load, Fans,interior equipment/lights, weather conditions like temperature, builing information like type/location and holiday information, predict the next day's energy consumption.


- Cooling load, Heating, Fans, interior, lights, building, location, Energy, timestamp


### Steps completed
#### Collected the energy consumption data, building metadata, weather data and holiday data for 2004
#### Merged all of these datasets and stored the wrangled dataset in csv
#### Performed EDA (Scatter plots, histograms) and feature analysis to understand the data shape and feature correlation
#### Used Feature engineering techniques like one-hot encoding to prepare the dataset for model evaluation
#### Passed the dataset through different models to evaluate r2 score and rmse
#### Used Regularization methods to prevent model overfitting
#### The best performing models so far were RidgeCV and Lasso
#### Added a model for the City of Seattle
#### Heat maps, Day/Month/temperature
#### visualizing the model with data points based on Energy consumption vs temperature
#### Cooling load (T), Heating (T), Fans (T), interior (T), lights (T), building, location, Energy (T + 1), Energy(T-1), delta((T), (T-1))

# TODO
- * PCA and Manifold for reducing dimensions into two vectors, to visualize data
- * Model evaluation with YellowBrick
- Hyperparameter tuning (Alpha value)
- * Story/Paper and slides

We can use:
LinearRegression(normalize=True)

Need to use database tables for storing and retrieving data

