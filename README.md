# open-rdg-api-mcp

[![Unofficial Project](https://img.shields.io/badge/Unofficial-purple)](#disclaimer) 

## Disclaimer <a name="disclaimer"></a>
This is an **independent, open-source experiment**.  
It is **not** affiliated with, endorsed by, or sponsored by the U.S. Department of Agriculture (USDA) or USDA Rural Development

Data retrieved through this project remains subject to the terms of the original data providers (e.g., USDA Rural Development).

## Goal
The goal of this project is to make a usable proof of concept for getting Tableau dashboard data into a local mcp server. 

Can make your local LLM get quick USDA Rural Development data with natural language rather than reling on the clicking in the Tableau dashboard. 

## Status
Things are 'working' with 2025 data from data download page, but getting all of the data from that page proves to be a very slow process. I updated the download_data.py to start to prep to get the data from the Overview page instead. That will still allow all of the summary data but no individual details. Maybe I'll get both. IDK. 

## Use
1. fork and clone this repo
2. `uv sync` # I may make this into a docker container
3. `uv run fetch/download_data.py` # or download Export to CSV manually and put in /data dir under open-rdg-api-mcp
4. `uv run api/data_processor.py` # this makes the csv into .db
5. `uv run api/server.py` # this makes the api endpoints available on localhost:8000 (already better)
6. Now for the good part. Once you have all of that done, you should be able to use the MCP Tool
    a. In MCP Inspector
    b. In your favorite LLM; for example, for Claude Desktop, you would add it to the config file

### How to use MCP Inspector
In a new terminal window (not the one that you have the `uv run api/server.py` running in), run `mcp-inspector uv run mcp/server.py`. 
A new browser window should open for you. Click 'Connect' on the left side, and then at the top, find 'Tools'. You can click on 'List Tools' and you will see all of the tools that we listed in `mcp/server.py`. This allows you to quickly test differnet combinations of requests even if you do not have a desktop llm. It will not show you LLM decisions. 

The browser window view is forced into sections, so it may seem a little clunky - you are ultimately interested in using the tools shown in the middle and then putting any arguments in and clicking 'run tool' on the right. Your result will be at the bottom on the right. 

![The top part of a visible window when using mcp inspector in teh browser](https://github.com/monkeywithacupcake/open-rdg-api-mcp/blob/main/_img/mcp_inspector_top.jpg?raw=true)
![The part of a visible window when using mcp inspector in the browser scrolling a little ways down](https://github.com/monkeywithacupcake/open-rdg-api-mcp/blob/main/_img/mcp_inspector_scroll.jpg?raw=true)


### How to add the mcp servr to your config file
Your chatbot should tell you where to find this, in Claude Desktop, you can go to Settings, Developer, and it will show you where the config file is. Or, you can search for information on mcp servers in your chatbot and get the details that work for you.

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
I finished all of the setup steps, and I added the mcp server to Claude Desktop. It ended up querying usda_rural_data mcp server 8 times, using all three of the tools. 

**Prompt:**
How may investments did USDA Rural Development make in WA in 2025, and what portion of them were for water?

**Response:**

*Claude sent several intermediate responses as it looked for my answer. Some of its intermediate responses suggest that the mcp server or the api are not 100% obvious to LLMs on how to handle. You can see this in how it keeps telling me about results and what is being suggested.* 

> I'll help you find information about USDA Rural Development investments in Washington state. Let me check the available data for 2025 and see what portion was allocated to water-related programs.
>
> Let me check what data is available and the valid fiscal years in the dataset.
>
> Perfect! Now I can see that 2025 data is available. Let me get the Washington state investment data for fiscal year 2025.
>
> Let me try without specifying the fiscal year since 2025 appears to be the only year available.
>
> Now let me get the breakdown by program areas to see how much was allocated to water-related programs.
>
> The result shows all 410 investments, which suggests all of Washington's investments in the available data may be in the Water and Environmental category. Let me verify this by getting a comparison across all program areas for Washington.
>
> Let me try a different approach and get detailed records to see the program breakdown.
>
> Great! Now I can see the different program areas. Let me check specifically for water-related investments and get a better breakdown by counting investments in each program area.
>
> Perfect! Now I have the complete picture. Let me provide you with the comprehensive answer.
>   
> Based on the USDA Rural Development data for Washington state in fiscal year 2025:
>
> **Total Investments:** 410 investments totaling $550,421,658
>
> **Water-Related Investments:** 1 investment totaling $634,000
>- This represents **0.24%** of total investments by count (1 out of 410)
>- This represents **0.12%** of total investment dollars ($634,000 out of $550.4 million)
>
> The single water investment was:
>- **Recipient:** City of Ilwaco (Pacific County)
>- **Program:** Water and Environmental Program - Direct Loan
>- **Amount:** $634,000
>- **Purpose:** Water meters project
>
> The vast majority of USDA Rural Development investments in Washington in 2025 were in other program areas, particularly:
>- Electric Programs (including major clean energy investments)
>- Business Programs
>- Single Family Housing
>- Telecommunications Programs
>- Community Facilities
> 
> So while Washington received significant USDA Rural Development funding overall, water and environmental projects represented a very small portion of the total investments in 2025.