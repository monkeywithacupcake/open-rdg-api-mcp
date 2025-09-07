# open-rdg-api-mcp

[![Unofficial Project](https://img.shields.io/badge/Unofficial-purple)](#disclaimer)

## Disclaimer <a name="disclaimer"></a>
This is an **independent, open-source experiment**.  
It is **not** affiliated with, endorsed by, or sponsored by the U.S. Department of Agriculture (USDA) or USDA Rural Development

Data retrieved through this project remains subject to the terms of the original data providers (e.g., USDA Rural Development).

## Goal
The goal of this project is to make a usable proof of concept for getting Tableau dashboard data into a local mcp server. 

## Steps
1. Get data from the website into a local file
2.  Make an API out of that local file
3.  Make an MCP Server consuming that API


## Current Step - Get Data

it's a bit of a pain because there is not an API, and I can't figure out how to use the Tableau dashboard programmatically. Plan is to just click the Export to CSV. It makes a popup. I haven't gotten it to download yet, but I'm close.
   - Navigate to filtered data page
   - Click "Export to CSV" 
   - Handle popup and click "Download"
   - Handle new tab and accept download
   - Save file to local storage

This is our button
   <a href="https://publicdashboards.dl.usda.gov/vizql/t/RD_PUB/w/DataDownload/v/Data/tempfile/sessions/B650447762164745A10607D1E0497B8E-1:0/?key=2452276633&amp;keepfile=yes&amp;attachment=yes" target="_blank" class="tabDownloadFileButton" data-test-id="DownloadLink" role="button" style="text-decoration: none; word-break: keep-all; white-space: nowrap;">Download</a>


