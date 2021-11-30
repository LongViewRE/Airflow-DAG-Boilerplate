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
