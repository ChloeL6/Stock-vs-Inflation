# Tech Stocks

#### By [Ruben Giosa](https://github.com/rgiosa10), [Chloe (Yen Chi) Le](https://github.com/ChloeL6), [Philip Kendal](https://github.com/philiprobertovich)

#### This repo showcases working as a team to build an ETL pipeline and create visualizations using Python, SQL, Airflow, Spark, Astro CLI, BigQuery and Looker Studio.

<br>

## Technologies Used

* Python
* Jupyter
* Airflow
* Astro CLI
* Spark
* BigQuery
* Looker Studio
* SQL
* Pandas
* Git
* Markdown
* NumPy
* `.gitignore`
* `requirements.txt`
  
</br>

## Datasets Used

1. [Big Tech Stock Prices](https://www.kaggle.com/datasets/evangower/big-tech-stock-prices)

</br>

## Description



#### Architectural diagram:


<br>

#### Data Model:


<br>

<br>

#### Data Pipeline:
<img src="imgs/ETL_pipeline.png" alt="Architectural diagram" width="640"/>

<br>

### ETL Construction:


<br>

[Chloe](https://github.com/ChloeL6) worked on profiling, cleaning and transformations for the [Big Tech Stock Prices](https://www.kaggle.com/datasets/evangower/big-tech-stock-prices) data set create the `fct_emissions` table. Upon completion it was loaded to BigQuery.

<br>

[Ruben](https://github.com/rgiosa10) performed profiling, cleaning and transformations on the [World Energy Consumption](https://www.kaggle.com/datasets/pralabhpoudel/world-energy-consumption) to compile both the `fct_gdp` and `fct_consump` tables. Upon completion it was loaded to BigQuery. He also owned and authored the `README.md`.

<br>

### Visualizations:
Once the datasets were cleaned and consolidated, the team created data visualizations and analysis (using Looker Studio) leveraging the constructed dimension and fact tables outlined above. 

Below is a combo chart that was put together by Ruben that shows GDP compared to Population and Energy Consumption (click on image of chart to use dashboard):

[<img src="imgs/GDP_pop_con_Looker_graph.png">](https://datastudio.google.com/embed/reporting/dbe92c8b-ccd3-41d9-b269-5964eb9717c3/page/f94CD)

While Github disables iframe, which allows embedding of the report on markdown files, I have included the code below for users that clone the project. 

```
<iframe width="600" height="450" src="https://datastudio.google.com/embed/reporting/dbe92c8b-ccd3-41d9-b269-5964eb9717c3/page/f94CD" frameborder="0" style="border:0" allowfullscreen></iframe>
```

The scale of GDP (trillions of dollars), population (billions), and Energy consumption (thousands of terawatt-hours) posed an issue for the visualization, but by embedding this report the user is able to see the individual values for energy consumption which shows the consistent trend that population and energy consumption growth align with the growth of GDP. The chart is dynamic in that it allows users to filter for country and timeframe. Also the timeframe of 1965 through 2016 was chosen as consumption data prior to 1965 was missing and GDP data goes up to 2016.

<br>

Below is a line chart by Ruben that shows total global energy consumption compared to CO2 emissions (click on image of chart to use dashboard):

[<img src="imgs/Con_Em_Looker_graph.png">](https://datastudio.google.com/embed/reporting/dbe92c8b-ccd3-41d9-b269-5964eb9717c3/page/f94CD)

As called out above Github disables iframe, but I have included the code below for reference:

```
<iframe width="600" height="450" src="https://datastudio.google.com/embed/reporting/b7d972c6-7faf-4c78-948c-614945f42350/page/Io6CD" frameborder="0" style="border:0" allowfullscreen></iframe>
```

The chart leverages different scales for the left and right y-axis to better show the correlation between emissions and energy consumption over time (1965 - 2019). The chart is dynamic in that it allows users to filter for country and timeframe.

<br>

Chloe put together two line graphs that 1) plots the global CO2 emissions over time with total emissions and each type of emission producer and 2) total consumption compared to renewable consumption (click on the image of either chart to use dashboard): 
 
[<img src="imgs/cl_global_CO2_emissions.png">](https://datastudio.google.com/embed/reporting/8a085df7-5101-4878-8c2c-8c6230de60d2/page/p_rh0ezxzj2c)

[<img src="imgs/cl_total_consump_vs_renewable.png">](https://datastudio.google.com/embed/reporting/8a085df7-5101-4878-8c2c-8c6230de60d2/page/p_rh0ezxzj2c)

<br>

Overall, the team was able to limit the amount of merge conflicts by working on independent notebooks and assigning different tasks (e.g. Each focused on constructing specific dimension and fact tables, etc.). One challenge we came across was setting up a BigQuery project and granting access to each user, this was a great learning experience for the team as we set up Service Accounts with authorization keys for each user. 

## Setup/Installation Requirements

* Go to https://github.com/philiprobertovich/team-week3.git to find the specific repository for this website.
* Then open your terminal. I recommend going to your Desktop directory:
    ```bash
    cd Desktop
    ```
* Then clone the repository by inputting: 
  ```bash
  git clone https://github.com/philiprobertovich/team-week3.git
  ```
* Go to the new directory or open the directory folder on your desktop:
  ```bash
  cd team-week3
  ```
* Once in the directory you will need to set up a virtual environment in your terminal:
  ```bash
  python3.7 -m venv venv
  ```
* Then activate the environment:
  ```bash
  source venv/bin/activate
  ```
* Install the necessary items with requirements.txt:
  ```bash
    pip install -r requirements.txt
  ```
* Download the necessary csv files listed in the Datasets Used section
* With your virtual environment now enabled with proper requirements, open the directory:
  ```bash
  code .
  ```
* Upon launch please update the Google Cloud client and project details to configure it to load to your project

</br>

## Known Bugs

* No known bugs

<br>

## License

MIT License

Copyright (c) 2022 Ruben Giosa, Philip Kendal, Chloe (Yen Chi) Le

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

</br>
