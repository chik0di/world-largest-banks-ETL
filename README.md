<h1 align="center">Global Bank Market Capitalization ETL Pipeline</h1>
   
![Pipeline](https://i.imgur.com/nNukcJ0.png)

A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements.

Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in various currencies per current exchange rate information.

You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.


<h1 align="center">Objectives</h1>

- Extract current exchange rate information from [x-rates](https://www.x-rates.com/).

- Extract the tabular information from the given URL for top banks by Market Capitalization, and save it to a data frame.

   URL - https://en.wikipedia.org/wiki/List_of_largest_banks
  
- Transform the data frame by adding columns for Market Capitalization in the available currencies, rounded to 2 decimal places, based on the exchange rate information.

- Load the transformed data frame to an output CSV file.

- Load the transformed data frame to an SQLite database server as a table.

- Run queries on the database table.

- Run the following queries on the database table:
  
  a. Extract the information for the London office, that is Name and MC_GBP_Billion

  b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion

  c. Extract the information for New Delhi office, that is Name and MC_INR_Billion
  
- Log the progress of the code.

- While executing the data initialization commands and function calls, maintain appropriate log entries.


<h1 align="center">Initial setup</h1>

The libraries needed to run the script are as follows:

- requests - used for accessing the information from the URL.

- bs4 - containing the BeautifulSoup function used for webscraping.

- pandas - used for processing the extracted data, storing it to required formats and communicating with the databases.

- sqlite3 - required to create a database server connection.

- numpy - required for the mathematical rounding operation as required in the objectives.

- datetime - The library containing the function datetime used for extracting the timestamp for logging purposes.


<h3 align="right">Assigned By</h3>
<p align="right">
  <a href="https://www.coursera.org/account/accomplishments/verify/TG89DJFGV7VD?utm_source=link&utm_medium=certificate&utm_content=cert_image&utm_campaign=sharing_cta&utm_product=course" title="View Certificate from IBM on Coursera">
    <img width="100" src="https://img.icons8.com/nolan/64/ibm.png" alt="IBM" />
  </a>
</p>

