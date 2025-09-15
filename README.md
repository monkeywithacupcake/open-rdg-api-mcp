# open-rdg-api-mcp

![Version 0.2.0](https://img.shields.io/badge/Version_0.2.0-grey)
[![Unofficial Project](https://img.shields.io/badge/Unofficial-purple)](#disclaimer) 

## Disclaimer <a name="disclaimer"></a>
This is an **independent, open-source experiment**.  
It is **not** affiliated with, endorsed by, or sponsored by the U.S. Department of Agriculture (USDA) or USDA Rural Development

Data retrieved through this project remains subject to the terms of the original data provider (e.g., USDA Rural Development).

## Goal
The goal of this project is to make a usable proof of concept for getting Tableau dashboard data into a local mcp server. 

Can make your local LLM get quick USDA Rural Development data with natural language rather than relying on the clicking in the Tableau dashboard. 

## Use

Sadly, because the data are in a Tableau viz and there is no API on the site, the *fastest* way to query the data with natural language is by first building a local API, which means that you need to get all of the data and then host your own api. Despite having several steps, the whole of the process should be straightforward.

1. fork and clone this repo
2. `uv sync`
3. `uv run fetch/download_data.py` # or download Export to CSV manually and put in /data dir under open-rdg-api-mcp
  - hist (10 year) summary data from [hist_url](https://www.rd.usda.gov/rural-data-gateway/rural-investments)
  - detail (defaults to current year) data from [detail_url](https://www.rd.usda.gov/rural-data-gateway/rural-investments/data)
4. `uv run api/data_processor.py` # this makes the csv into .db
5. `uv run api/server.py` # this makes the api endpoints available on localhost:8000 (already better)
6. Now for the good part. Once you have all of that done, you should be able to use the MCP Tool
    a. In MCP Inspector
    b. In your favorite LLM; for example, for Claude Desktop, you would add it to the config file

### How to use MCP Inspector
In a new terminal window (not the one that you have the `uv run api/server.py` running in), run `mcp-inspector uv run mcp/server.py`. 
A new browser window should open for you. Click 'Connect' on the left side, and then at the top, find 'Tools'. You can click on 'List Tools' and you will see all of the tools that we listed in `mcp/server.py`. This allows you to quickly test differnet combinations of requests even if you do not have a desktop llm. It will not show you LLM decisions. 

The browser window view is forced into sections, so it may seem a little clunky - you are ultimately interested in using the tools shown in the middle and then putting any arguments in and clicking 'run tool' on the right. Your result will be at the bottom on the right. 

![The top part of a visible window when using mcp inspector in the browser](https://github.com/monkeywithacupcake/open-rdg-api-mcp/blob/main/_img/mcp_inspector_top.jpg?raw=true)
![The part of a visible window when using mcp inspector in the browser scrolling a little ways down](https://github.com/monkeywithacupcake/open-rdg-api-mcp/blob/main/_img/mcp_inspector_scroll.jpg?raw=true)


### How to add the mcp servr to your config file
Your LLM should tell you where to find this, in Claude Desktop, you can go to Settings, Developer, and it will show you where the config file is. Or, you can search for information on mcp servers for your specific setup and get the details that work for you.

The json that you need is this: 
*note: i am on a mac, windows users may have to play with paths*

```json
"mcpServers": {
    "usda-rural-data": {
      "command": "uv", 
      "args": [
        "--directory",
        "/full-path-to/open-rdg-api-mcp",
        "run",
        "mcp/server.py"
      ],
      "env": {}
    }
  }
```
*note: I had to use `"full-path-to/bin/uv"` in the command json, but depending on your system, you may be able to just use uv. If the json below fails for you, go into your project folder in the shell and use `which uv` to find your full path*

It might ask you to give permission to use your mcpserver. I show this here:

![A screenshot of Claude Desktop asking permission to use usda-rural-data](https://github.com/monkeywithacupcake/open-rdg-api-mcp/blob/main/_img/claude_desktop_using_mcp.jpg?raw=true)



## Steps in creating this project -- probably not super interesting after everything is in here
1. Get data from the website into a local file
    - this was painful and didn't have to be - note to people trying to do this, use `playwright`. I am on a mac and have had some issues with chromium for this kind of thing, so I used firefox (but playwright still uses chromium to build your code, that seems to work for me). The actual code I put in my shell to get the python to click on the download button was `npx playwright codegen --browser=firefox https://www.rd.usda.gov/rural-data-gateway/rural-investments/data`. Keeping this detail because it was annoying and really should not have been that hard.
   - Backup if it doesn't work, or user gets too annoyed at playwright. User can manually download the csv into `/data`. the only limitation on it is that it has to be a .csv with the expected columns based on the website, and lack of naming constraint means we don't have to worry about it.
2.  Process data into local database
    - only process newest file
3.  Make an API out of that local file
    - add tests, so can make sure is running as expected
4.  Make an MCP Server consuming that API


## Example of actual use
I finished all of the setup steps, and I added the mcp server to Claude Desktop. It ended up querying usda_rural_data mcp server more than 10 times (you can see the calls below) 

**Prompt:**
How may investments did USDA Rural Development make in WA in 2024, and what portion of them were for water?

**Response:**

*Claude sent several intermediate responses as it looked for my answer. Some of its intermediate responses suggest that the mcp server or the api are not 100% obvious to LLMs on how to handle. You can see this in how it keeps telling me about results and what is being suggested.* 

> I'll help you find information about USDA Rural Development investments in Washington state for 2024, with a focus on water-related investments.
>  
> Let me try a different approach to get the 2024 data for Washington state:Let me first check what data is available and the proper format for fiscal years:
> 
> I can see that the data covers fiscal years 2015-2025, but 2024 appears to be available in the historical summary dataset. Let me try getting Washington state data to see what's available for 2024:
>  
> Now let me get the specific data for 2024 and look at the program breakdown to identify water-related investments:Let me try without specifying the fiscal year to see what years are actually available:
>  
> Great! I can see 2024 data is available. Now let me get all the 2024 data for Washington to see all program areas and specifically identify water investments:
> 
> Perfect! I can see the 2024 data for Washington. Let me search specifically for water-related programs to get a more comprehensive view:
> 
> Based on the data I've retrieved, here's what I found about USDA Rural Development investments in Washington state for fiscal year 2024:
> 
> ## Total USDA Rural Development Investments in Washington (2024)
> 
> From the data, I can identify the following 2024 investments in Washington state:
> 
> **Total investments across all programs:** At least **723 investments** totaling **$226.52 million**
> 
> This includes:
> - **Single Family Housing:** 210 investments ($69.29 million)
> - **Business Programs:** 160 investments ($69.28 million) 
> - **Multifamily Housing:** 342 investments ($56.92 million)
> - **Water and Environmental:** 11 investments ($31.02 million)
> 
> ## Water-Related Investments
> 
> **Water investments in 2024:** **11 investments** totaling **$31.02 million**
> 
> **Portion for water:** The water and environmental investments represented:
> - **1.5%** of the total number of investments (11 out of 723)
> - **13.7%** of the total investment dollars ($31.02M out of $226.52M)
> 
> The "Water and Environmental" program area covers water infrastructure, wastewater systems, solid waste management, and environmental improvements - all critical for rural communities' health and economic development.
> 
> It's worth noting that this represents the investments I can identify from the summary data. There may be additional water-related investments within other program categories that aren't specifically categorized as "Water and Environmental."


for record later, these are teh calls that it did:
- INFO:     127.0.0.1:54872 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54872 - "GET /data/summary HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54872 - "GET /data/summary HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54872 - "GET /investments?limit=100&offset=0 HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54872 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54872 - "GET /aggregations/states?state=Washington HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54872 - "GET /data/summary HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54874 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54874 - "GET /summary?limit=20&offset=0&state=Washington HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54874 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54874 - "GET /summary?limit=50&offset=0&state=Washington HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54874 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:54874 - "GET /aggregations/states?state=Washington HTTP/1.1" 200 OK


**NOTE**
After adding the additional info and mcp Resource, it took fewer requests
- INFO:     127.0.0.1:50882 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50882 - "GET /data/summary HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50882 - "GET /data/summary HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50882 - "GET /investments?limit=100&offset=0 HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50884 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50884 - "GET /summary?limit=20&offset=0&state=Washington HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50886 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50886 - "GET /summary?limit=50&offset=0&state=Washington HTTP/1.1" 200 OK

some additional testing also show that Claude gets a lot of information and then decides on its own what to do with ith rather than, say, use the comparison tools.
But it is getting the right data.

When asked to compare PNW states in 2023 and 2024, it only called 3 main endpoints (and health)
- INFO:     127.0.0.1:50893 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50893 - "GET /summary?limit=100&offset=0&state=Washington HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50893 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50893 - "GET /summary?limit=100&offset=0&state=Oregon HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50893 - "GET /health HTTP/1.1" 200 OK
- INFO:     127.0.0.1:50893 - "GET /summary?limit=100&offset=0&state=Idaho HTTP/1.1" 200 OK