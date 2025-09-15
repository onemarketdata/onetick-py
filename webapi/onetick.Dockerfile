ARG WEBAPI_SERVER_ONETICK_BUILD

FROM 977320806745.dkr.ecr.us-east-1.amazonaws.com/onetick/server:${WEBAPI_SERVER_ONETICK_BUILD}

COPY ./webapi/initial_configs /initial_configs

# Gets license on every run
ENTRYPOINT ["/bin/bash", "/scripts/docker-entrypoint.sh"]
