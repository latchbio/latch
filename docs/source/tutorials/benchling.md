# Benchling Sync

Latch Registry integrates with Benchling API to import your entity and plate schemas as a Project to Latch. This document walks through the process of setting up Benchling Integration and using it to import your data.

## Setup Benchling Sync

The integration works by using your Benchling developer API key.

1. Follow official [Benchling tutorial](https://help.benchling.com/hc/en-us/articles/9714802977805-Access-the-Benchling-Developer-Platform#h_2962600be3) to get your personal user API key.
2. In Latch Console, go to Workspace Settings > Developer, and click on Benchling
![Add Benchling Credentials](../assets/registry/benchling/credentials-1.png)
3. For `BENCHLING_TENANT_URL` put in your tennant URL. Ex. `https://latch.benchling.com/`.
4. For `BENCHLING_API_KEY` put in your Benchling API key. Ex. `sk_example_key`
5. Click on `Submit` to save your Benchling credentials which will securly store your credentials in Amazon Secrets Manager.
![Add Secret](../assets/registry/benchling/credentials-2.png)
6. You will see your Benchling secrets added to the `Workspace Secrets` section.
![See your Benchling Secrets](../assets/registry/benchling/credentials-3.png)

## Importing Data Using Benchling Sync
After successfully adding your Benchling credentials to Latch, you can go to `Registry` and import your data as a new project.

1. Go to `Registry`.

2. Create a new project by clicking `New Project` in the sidebar.
![New Project](../assets/registry/benchling/data-1.png)
![Sync Button](../assets/registry/benchling/data-2.png)

3. Click on `Benchling Data Sync` in the top right corner.
![Sync Button](../assets/registry/benchling/data-3.png)


4. Select the project to sync data into.
![Sync Button](../assets/registry/benchling/data-4.png)
![Sync Button](../assets/registry/benchling/data-5.png)

5. Use the selector to switch between Plate and Entity schemas.
![Sync Button](../assets/registry/benchling/data-6.png)

6. Check Benchling Schemas and columns that you want to sync. These schemas will be imported into the tables under the project that you have selected in step 4. If the entity schema you selected, links to another schemas, those will be selected for an import as well. It is crucial to import all related schemas to properly link Registry tables.
![Sync Button](../assets/registry/benchling/data-7.png)


7. (Optional) Enable automatic sync. If this option is enabled, your data will be automatically synced from Benchling to Latch every 30 minutes.
![Sync Button](../assets/registry/benchling/data-8.png)

8. Click `Save Sync Settings` to save the state of your sync.

9. Click `Manual Sync` to manually import all the schemas that you have selected into Latch. Depending on the size and the amount of schemas that you are importing, this might take a couple of minutes. Please keep the tab with the importer open. If you need to use the rest of the platform, please open a new tab and go to console page there.

10. After the sync exits, all of the schemas that you have selected will be available in the specified project.
![Sync Button](../assets/registry/benchling/data-9.png)

See how you can use [Registry](overview.md) to better analyze and search through your Benchling data.
