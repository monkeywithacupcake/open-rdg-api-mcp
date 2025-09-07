# open-rdg-api-mcp

[![Unofficial Project](https://img.shields.io/badge/Unofficial-purple)](#disclaimer)

## Disclaimer <a name="disclaimer"></a>
This is an **independent, open-source experiment**.  
It is **not** affiliated with, endorsed by, or sponsored by the U.S. Department of Agriculture (USDA) or USDA Rural Development

Data retrieved through this project remains subject to the terms of the original data providers (e.g., USDA Rural Development).

## Goal
The goal of this project is to make a usable proof of concept for getting Tableau dashboard data into a local mcp server. 

Can make your local LLM into a genie to get quick USDA Rural Development data with natural language rather than reling on the clicking in the Tableau dashboard. 

## Steps
1. Get data from the website into a local file
    - this was painful and didn't have to be - note to people trying to do this, use `playwright`. I am on a mac and have had some issues with chromium for this kind of thing, so I used firefox (but playwright still uses chromium to build your code, that seems to work for me). The actual code I put in my shell to get the python to click on the download button was `npx playwright codegen --browser=firefox https://www.rd.usda.gov/rural-data-gateway/rural-investments/data`. Keeping this detail because it was annoying and really should not have been that hard.
   - Backup if it doesn't work, or user gets too annoyed at playwright. User can manually download the csv into `/data`. the only limitation on it is that it has to be a .csv with the expected columns based on the website, and lack of naming constraint means we don't have to worry about it.
2.  Process data into local database
    - only process newest file
3.  Make an API out of that local file
4.  Make an MCP Server consuming that API


## Current Step - API

- this should be straightforward