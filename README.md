# EnergyScience
Cohort 14 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.
This project will attempt to build a model that forecasts thermal load for a building.

## Data instance
Given the Cooling load, heating load, and various other energy consumption sources like Fans, interior equipment/lights, weather conditions like temperature, builing information like type/location and holiday information, can we predict the overall energy load in a building on a given day

### Steps completed
#### Collected the energy consumption data, building metadata, weather data and holiday data for 2004
#### Merged all of these datasets and stored the wrangled dataset in csv
#### Performed EDA (Scatter plots, histograms) and feature analysis to understand the data shape and feature correlation
#### Used Feature engineering techniques like one-hot encoding to prepare the dataset for model evaluation
#### Passed the dataset through different models to evaluate r2 score and rmse
#### Used Regularization methods to prevent model overfitting
#### The best performing models so far were RidgeCV and Lasso

# TODO
- * Evaluate SVM
- More Feature Analysis/EDA
- * Model evaluation with YellowBrick
- Hyperparameter tuning (Alpha value)
- * Story/Paper and slides
- Maybe a model per city (tentative)

We can use:
LinearRegression(normalize=True)

