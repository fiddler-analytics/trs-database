# TRS Database

[![Run on Repl.it](https://repl.it/badge/github/fiddler-analytics/trs-database)](https://repl.it/github/fiddler-analytics/trs-database)

I put together the scripts in this repo because Nathan made clear that everyone (bafflingly) wants to update the database and run reports at 2AM. To get started, click the badge above to create a Repl.it based on this repo. Once it's done setting up, create a `.env` file with the following content. After that, you're good to go. Just click `Run` to get started.

```
PG_SCHEMA="<pg-schema>"
PG_DATABASE="<pg-database>"
PG_HOST="<pg-host>"
PG_USER="<pg-user>"
PG_PASS="<pg-pass"

EVENTBRITE_OAUTH="<eventbrite-oauth>"
EVENTBRITE_ORG="<eventbrite-org>"
```
