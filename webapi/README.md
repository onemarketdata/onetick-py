# WebAPI testing suite (work-in-progress)

WebAPI testing in implemented by using docker-compose with two containers:

1. `tickserver` with instantiated WebAPI server
2. `otpwebapi` with `onetick-py` code

We need to separate filesystems to correctly test
whether the `onetick.query_webapi` library is able to upload OTQ/CSV to the server.
But, at the same time, we need to share a configuration files (config/locator/acl) with up-to-date list of DBs.
Each time new otp.Session is created or locator/acl files are updated (e.g.: dbs added),
onetick-py run RELOAD_CONFIG EP on server to update the configuration.

To force `onetick-py` using shared configuration folder, set env `OTP_WEBAPI_TEST_MODE=1`.
Also, `WEBAPI_TEST_MODE_SHARED_CONFIG` could be set to desired path, by default it is `/shared_config`.

## Usage

Start the testing stand:

```bash
docker login -u AWS -p $(aws ecr get-login-password --region us-east-1) 977320806745.dkr.ecr.us-east-1.amazonaws.com
# these variables can be parametrized by setting env variables:
# MAIN_TS_PORT=12345
# HTTP_SERVER_PORT=48028
# DEBUGPY_PORT=48029
# DEBUGPY_PORT=48029
# WEBAPI_SERVER_ONETICK_BUILD=20250727-0
# ONETICK_QUERY_WEBAPI_VERSION=20250727.0.0
docker-compose -f docker-compose-webapi.yml up -d
```

Run tests:

```bash
docker-compose exec otpwebapi sudo -E -u onetick pytest -Wdefault
```

## Debugging tests

By default docker will expose port 48029 to the host machine, so you can use it for debugging.
Add the following code to the place where you want to start debugging (breakpoint included):

```python
import debugpy
debugpy.listen(("0.0.0.0", 48029))
print("Waiting for debugger attach")
debugpy.wait_for_client()
debugpy.breakpoint()
```

Now you can run the tests and attach the debugger to the running container.

If you're using VSCode, add the following configuration to `.vscode/launch.json` into `configurations` array:

```json
        {
            "name": "Python: Remote Attach",
            "type": "debugpy",
            "request": "attach",
            "connect": {
                "host": "localhost",
                "port": 48029
            },
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "."
                }
            ],
            "justMyCode": false
        }
```

Then run the tests and attach the debugger by clicking on the green play button
in the debug panel with the name `Python: Remote Attach` selected.

## TODO

- [x] Write docs for WebAPI support in OTP.
- [x] Make `webapi-compatibility` job running only in MR.
- [x] Replace `onetick.Dockerfile` with actual image from ECR.
- [x] [PY-964](https://onemarketdata.atlassian.net/browse/PY-964)
  remove `onetick.query_webapi-20240330.0.0-py3-none-any.whl`
  file from repo and use package from Onetick distribution ()
- [ ] [PY-964](https://onemarketdata.atlassian.net/browse/PY-965)
  add OQD UDEPs (and DLLs?) to `tickserver` container and test it (`test_oqd.py`).
  [Installation instructions](https://onemarketdata.atlassian.net/browse/KB-415)
- [x] understand `test_reload_locator_with_derived_database` case and fix it.
