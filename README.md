# Tech Stocks

#### By [Ruben Giosa](https://www.linkedin.com/in/rubengiosa/), [Chloe (Yen Chi) Le](https://github.com/ChloeL6), [Philip Kendal](https://github.com/philiprobertovich)

#### This repo showcases working as a team to build an ETL pipeline and create visualizations using Python, SQL, Airflow, Spark, Astro CLI, BigQuery and Looker Studio.

<br>

## Technologies Used

* Python
* Jupyter
* Airflow
* BigQuery
* Looker Studio
* SQL
* Pandas
* Git
* Markdown
* `.gitignore`
* `requirements.txt`
  
</br>

## Datasets Used

1. [Big Tech Stock Prices](https://www.kaggle.com/datasets/evangower/big-tech-stock-prices)
2. [Bitcoin Prices Dataset](https://www.kaggle.com/datasets/yasserh/bitcoin-prices-dataset)
3. [M1, M2 and other Release Data, Monthly -in billions](https://www.federalreserve.gov/datadownload/Download.aspx?rel=H6&series=798e2796917702a5f8423426ba7e6b42&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package)

</br>

## Description


[<img src="imgs/stocks_m2.png" alt="stocks and M2 Supply" width="640"/>](https://lookerstudio.google.com/reporting/5d9a4269-f35d-4b46-a6b3-54bcbbb990a1)


<br>

#### DAGs of Data Pipeline:

<img src="imgs/rg_dag.png" alt="Airflow dag" width="680"/>


<br>

### ETL Construction:


<br>

[Chloe](https://github.com/ChloeL6) worked on profiling, cleaning and transformations for the [Big Tech Stock Prices](https://www.kaggle.com/datasets/evangower/big-tech-stock-prices) data set create the `fct_emissions` table. Upon completion it was loaded to BigQuery.

<br>

[Ruben](https://www.linkedin.com/in/rubengiosa/) created a DAG using Airflow to that performed profiling, cleaning and transformations on the [Big Tech Stock Prices](https://www.kaggle.com/datasets/evangower/big-tech-stock-prices), [Bitcoin Prices Dataset](https://www.kaggle.com/datasets/yasserh/bitcoin-prices-dataset), and [M1, M2 and other Release Data, Monthly -in billions](https://www.federalreserve.gov/datadownload/Download.aspx?rel=H6&series=798e2796917702a5f8423426ba7e6b42&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package) datasets once they are detected in the data directory. Once the transformations are completed these are complied into two `Parquet` files. Upon completion these two files are loaded to as tables into BigQuery. He also owned and authored the `README.md`.

<br>

### Visualizations:
Once the datasets were cleaned and consolidated, the team created data visualizations and analysis (using Looker Studio).

Below is a line graph that was put together by [Ruben](https://www.linkedin.com/in/rubengiosa/) that allows a user to look at the highest tech stock and Bitcoin prices by year (click on image of chart to use dashboard):

[<img src="imgs/stocks_no_btc.png" alt="stocks no btc snapshot" width="640"/>](https://lookerstudio.google.com/reporting/5d9a4269-f35d-4b46-a6b3-54bcbbb990a1)

The chart is dynamic in that it allows users to filter for specific months of each year and specific stock/bitcoin combinations.

<br>

Below is a line chart by [Ruben](https://www.linkedin.com/in/rubengiosa/) that shows the average tech stock and bitcoin price compared against average annual [M2 Money supply](https://en.wikipedia.org/wiki/Money_supply#:~:text=M2%20is%20a%20broader%20classification,large%20and%20long%2Dterm%20deposits.) (click on image of chart to use dashboard):

[<img src="imgs/Con_Em_Looker_graph.png">](https://datastudio.google.com/embed/reporting/dbe92c8b-ccd3-41d9-b269-5964eb9717c3/page/f94CD)

The chart leverages different scales for the left and right y-axis to better show the correlation between stock/bitcoin prices and M2 money supply over time. The chart is dynamic in that it allows users to filter for months and specific stocks and/or bitcoin.

<br>

Chloe put together two line graphs that 1) plots the global CO2 emissions over time with total emissions and each type of emission producer and 2) total consumption compared to renewable consumption (click on the image of either chart to use dashboard): 
 
[<img src="imgs/cl_global_CO2_emissions.png">](https://datastudio.google.com/embed/reporting/8a085df7-5101-4878-8c2c-8c6230de60d2/page/p_rh0ezxzj2c)

[<img src="imgs/cl_total_consump_vs_renewable.png">](https://datastudio.google.com/embed/reporting/8a085df7-5101-4878-8c2c-8c6230de60d2/page/p_rh0ezxzj2c)

<br>

Overall, the team was able to limit the amount of merge conflicts by working on independent notebooks and assigning different tasks (e.g. Each focused on constructing specific DAGs and python files, etc.). One challenge we came across was setting up a BigQuery project and granting access to each user, this was a great learning experience for the team as we set up Service Accounts with authorization keys for each user. 

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
