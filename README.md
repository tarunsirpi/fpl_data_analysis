# fpl_data_analysis

This project is built for analysing FPL data. 

The FPL Dataset includes the data retrieved from the FPL API for the analysis of Premier League fixtures, individual player statistics across different PL Teams.

More info about the dataset can be found here: Website - https://www.game-change.co.uk/2023/02/10/a-complete-guide-to-the-fantasy-premier-league-fpl-api/

Using the data available, analysis is done and data is cleaned. Feature engineering is done and the data is processed to make it suitable for training an ML model. 

Hyperparameter tuning is done to choose the algorithm and best parameters for training the model. Here, **Random Forest Regrerssion** is used to predict the points scored by a player with a Mean Absolute Error of 1.23 .

A web API is built using FastAPI, which can predict the points scored with a player name given as input. 

(Note: For more information on the Data Engineering architecture, refer to the ```Data-Engineering/README.md``` file.)


  * ```Data-Engineering/``` - contains files required for getting the data from FPL API.
  * ```artifacts/``` - contains CSV files for training data.
  * ```notebooks/``` - contains data analysis jupyter notebook file(s).
  * ```src/``` - contains python scripts and pipeline files for required training and prediction.
  * ```templates/``` - contains frontend template files.


## Setup git repo:

```
git init
git add .
git commit -m "commit messsage"
git branch -M main
git remote add origin <repo url>
git push -u origin main
```

## Setup for Anaconda environment:

Run the following to setup python environment for training the model,
```
conda create -p env_name python=3.8 -y
```
```
conda activate env_name
```
``` 
pip install -r requirements.txt
```

## Docker setup:

Run the following commands to create a docker image and run the FastAPI application on a container,

```
docker build -t image-name .
```
```
docker run -p 8010:8010 --name container-name image-name
```

check the link : http://localhost:8010/


For running the application without container, run the ```main.py``` file to start the FastAPI application.