# Airbyte Connector to Brønnøysundregistrene Updates 

## Local environment

This is for testing the connectors locally. We can stream the data to console. To stream 
data into a database, we use Airbyte together with docker containers of the connectors.

1. Clone this repo and cd into it

2. Create local python enviroment and source it. For example

```bash
python -m venv .venv
source .venv/bin/activate
```

(use `deactivate` to leave the environment).

3. Run install command: `python setup.py install`
This will install the airbyte-cdk into your local environment.

4. Add file `config.json` in folder `sample_files` and add necessary parameters for running the connectors:

```
{
    "batch_size": 10,
    "max_entries": 10000
}
```

5. Using the connector:

- For checking the connection
``` 
python main.py check --config sample_files/config.json
```

- For discovering the connection

``` 
python main.py discover --config sample_files/config.json
```

- For reading out the data (will stream data to console)
```
python main.py read --config sample_files/config.json --catalog sample_files/configured_catalog.json
```

- For reading out the data and saving the state (connector supports persisting states and incremental updates)
```
python main.py read --config sample_files/config.json --catalog sample_files/configured_catalog.json | grep STATE | tail -n 1 | jq .state.data > sample_files/state.json
```

- For reading data and initializing a state defined by a state file
```
python main.py read --config sample_files/config.json --catalog sample_files/configured_catalog.json --state sample_files/state.json
```


## Docker 

Building a docker image is required to use the connector together with the Airbyte Web App.
Also it allows to check if the connector works as intended in the way how it is used by the web app.

1. Clone repository and cd into connector directory.

2. Run 

```
docker build . -t airbyte/source-bronnoyregister:<tag>
```
with a tag, e.g., `tag=0.1` or `tag=dev`.

3. Populate `sample_files` as explained above.

4. The docker image is ready to use.

- For checking the connection
```
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bronnoyregister:dev check --config /sample_files/config.json
```
- For discovering the connection
```
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bronnoyregister:dev discover --config /sample_files/config.json
```
- For reading out the data (will stream data to console)
```
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bronnoyregister:dev read --config /sample_files/config.json --catalog /sample_files/configured_catalog.json
```
- For reading out the data and saving the state in sample files on the host machine (connector supports persisting states and incremental updates)
```
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bronnoyregister:dev read --config /sample_files/config.json --catalog /sample_files/configured_catalog.json | grep STATE | tail -n 1 | jq .state.data > sample_files/state.json
```
- For reading data and initializing a state defined by a state file
```
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bronnoyregister:dev read --config /sample_files/config.json --catalog /sample_files/configured_catalog.json --state /sample_files/state.json
```