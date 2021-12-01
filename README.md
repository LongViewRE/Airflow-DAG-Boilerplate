# Gerald Syncing
This repository will contain the functions to keep Gerald in sync with all systems/third party applications used at LongView. 

To work on this repo, clone it and create a local.settings.json file with the following structure:

    {
        "IsEncrypted": false,
        "Values": {
            "AzureWebJobsStorage": "UseDevelopmentStorage=true",
            "FUNCTIONS_WORKER_RUNTIME": "python",
            INSERT KEYS/TOKENS HERE...
        }
    }

### Deploying
As these functions use private Python packages, there are a couple extra steps to follow when deploying to Azure. See [this](https://sbdopmrentals.sharepoint.com/sites/IT/cda-wiki/SitePages/Using-Private-Python-Packages.aspx) for details
