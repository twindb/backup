### Regarding `dummy_env_vars.json.template`

## General purpose:

`dummy_env_vars.json.template` is a template that you can copy past into a `dummy_env_vars.json` file with your own
values defined.

It is meant to provide a pythonic mechanism to quickly, and easily, set up the necessary components for testing the
Azure blob storage extension to the twindb/backup project.

## Quick overview of the file's structure

#### Minimal key/value pairs for file to serve its purpose

For the example values, we use a [Flintstones](https://en.wikipedia.org/wiki/The_Flintstones) naming theme to aid
in making it clear where you should supplement your own values.

```json
{
    "os.environ": {
        "comments":  [
            "The `os.environ` key is a dict of environment variables that should be created prior to testing",
            "the general structure of this dict should look something like this: env_vars['os.environ']['destination_container']"
        ],
        "test_destination": {
            "comments":  [
                "The value associated with 'PRIMARY_TEST_CONN_STR' is just a placeholder but it also serves to show",
                "the expected structure of the connection string"
            ],
            "PRIMARY_TEST_CONN_STR": "DefaultEndpointsProtocol=https;AccountName=from_the_town_of_bedrock;AccountKey=hAVE+4+Ya8Ado/time+a+DAb4do/TIME+a+/Y4b4/d484/d0+tIMe==;EndpointSuffix=flintstones.meet.the.flintstones.net",
            "INTERVALS": ["hourly","daily","weekly","monthly","yearly"],
            "PATH_PARTS":["protocol","host","container","interval","media_type","fname_prefix","fname"],
            "COMPLETE_REMOTE_PATH_TEMPLATE": "{protocol}://{host_name}/{container_name}/{interval}/{media_type}/{fname_prefix}{fname}"
        }
    },
    "dummy_vals": {
        "comments": [
            "This is where we define container names and the blob paths under those containers for use in testing."
        ],
        "container_names": [
            "fred-of-buffalo-lodge",
            "barney-of-buffalo-lodge",
            "wilma-of-impossibly-good-figure",
            "betty-of-impossibly-good-figure"
        ],
        "fname_template": {
            "comments": [
                "this dict is used by tests/unittests/excluded_env_config/build_out_dummy_env.py",
                "to build a mock environment for testing."
            ],
            "optional_directory_prefix": "{interval}/mysql",
            "format_string": ["{child}{sep}{disposition}{sep}{item_type}.{extension}"],
            "template_parts": {
                "sep": ".",
                "child": "pebbles|bambam",
                "disposition": "likes|hates",
                "item_type": "dinosaurs|caves|cave_paintings",
                "extension": "txt"
            }
        }
    }
}


```

## Quick explanation of the component key/value pairs

```json
{
    "os.environ": {
        "comments": [
            "The `os.environ` key is a dict of environment variables that should be created prior to testing",
            "the general structure of this dict should look something like this: env_vars['os.environ']['destination_container']"
        ],
        "test_destination": {
            "PRIMARY_TEST_CONN_STR": "This should be the connection string for your target Azure subscription as defined here:\n https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal"
        }
    },
    "dummy_vals": {
        "comments": [
            "This is where we define container names and the blob paths under those containers for use in testing."
        ],
        "container_names": [
            "best",
            "not",
            "change",
            "unless",
            "you",
            "also",
            "change",
            "test",
            "scripts",
            "to",
            "match"
        ],
        "fname_template": {
            "comments": [
                "this dict is used by tests/unittests/excluded_env_config/build_out_dummy_env.py",
                "to build a mock environment for testing."
            ],
            "optional_directory_prefix": "{interval}/mysql",
            "format_string": [
                "{child}{sep}{disposition}{sep}{item_type}.{extension}"
            ],
            "template_parts": {
                "sep": ".",
                "child": "pebbles|bambam",
                "disposition": "likes|hates",
                "item_type": "dinosaurs|caves|cave_paintings",
                "extension": "txt"
            }
        }
    }
}
```

"the dictionaries that follow are examples of expected data structures": "key names inside chevrons, E.G. <>, are
optional and can be named however you like, all other's are minimum requirements\n\tall values are dummy examples and
should be replaced according to your own account details.",
