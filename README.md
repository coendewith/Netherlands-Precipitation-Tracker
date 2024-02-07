# Netherlands-Precipitation-Tracker
This repository contains a Python script designed to automatically collect and update precipitation data for major cities in the Netherlands. The data is fetched hourly and uploaded to a Google Sheet, making it readily available for analysis.

### View the dash here: https://docs.google.com/spreadsheets/d/1KuEZgfBt3Rflyw-j-m9BgHTfwuNYt-AHUplVLjV80ac/edit#gid=824926456


## Features
Hourly Data Collection: Precipitation data from various cities across the Netherlands is collected every hour.
Google Sheets Integration: Automated upload of data to a Google Sheet for easy access and analysis.
Cron Job Scheduling: The script is configured to run as a cron job, ensuring consistent data updates.
Historical Data Collection: An additional script runs daily at 11 PM to collect historical precipitation data.

## Technologies Used
Python
gspread and oauth2client for Google Sheets API interaction
BeautifulSoup for web scraping
Pandas for data manipulation
Requests for HTTP requests
