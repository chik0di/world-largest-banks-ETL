# Project Scenario
A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements.

Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in various currencies per current exchange rate information.

You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

# Objectives 
- Extract current exchange rate information from some website. 

- Extract the tabular information from the given URL for top banks by Market Capitalization, and save it to a data frame.

   URL - https://en.wikipedia.org/wiki/List_of_largest_banks
  
- Transform the data frame by adding columns for Market Capitalization in the available currencies, rounded to 2 decimal places, based on the exchange rate information.

- Load the transformed data frame to an output CSV file.

- Load the transformed data frame to an SQL database server as a table.

- Run queries on the database table.

- Run the following queries on the database table:
  
  a. Extract the information for the London office, that is Name and MC_GBP_Billion

  b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion

  c. Extract the information for New Delhi office, that is Name and MC_INR_Billion
  
- Log the progress of the code.

- While executing the data initialization commands and function calls, maintain appropriate log entries.
