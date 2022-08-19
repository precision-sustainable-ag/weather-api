# weather-api

The weather API serves weather data for the conterminous United States.

## Table of Contents:

- [Tech Stack](#tech-stack)
- [Features](#features)
- [Installation](#example2)

## Tech Stack

- Node.js

## Features

The API is documented at https://api.precisionsustainableag.org/weather/

## Local Installation Steps

**Prerequisites:**
1. You must be a member of the PSA team.
2. Your IP address must be added to the Postgres database that's serving the weather data. Contact @rickhitchcock with that information.
3. Node and NPM [Download Here](https://nodejs.org/en/download/)
4. Git [Download Here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
5. A code editor (we recommend VS Code) [Download Here](https://code.visualstudio.com/docs/setup/setup-overview)

**Steps:**
1. Open a new Terminal for Mac/Linux or Command Prompt for Windows
2. Move to the desired folder `cd /path/to/folder`
3. Clone this repository into that folder `git clone https://github.com/precision-sustainable-ag/weather-api`
4. From the Terminal/Command Prompt, move into the cloned directory `cd weather-api`
5. From the same command window, run `npm install` to install project dependencies. A full list of the dependencies can be found in package.json. If you are running on a windows machine, delete package-lock.json prior to running the below command. 
6. Create a file called .env in src/shared. The file will contain the below keys. This document is in the git ignore, so it (and your API keys) won't be pushed to the repository. Ask @rickhitchcock for the values of the keys
```
Weather|postgres|<weather server key>
GoogleAPI||<google api key>
```
7. After the dependencies have been installed and the .env file has been created, run `node index` to run the code locally.
8. Open http://localhost:1010/.

**Date Created:** 08/15/2022

**Date Last Modified:** 08/19/2022
