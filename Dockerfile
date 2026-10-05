ARG ONETICK_BUILD

# image is based on the ${ONETICK_BUILD} image
FROM ${ONETICK_BUILD}

ARG LOCAL_PIP_URL

# can be used to install fixed pandas+numpy version,
# as onetick-py has flexible requirements
ARG STRICT_DEPENDENCIES

RUN sudo apt-get update
RUN sudo apt-get install --no-install-recommends graphviz -y
RUN sudo apt-get install --no-install-recommends enchant-2 -y
RUN sudo apt-get install --no-install-recommends zip -y
RUN sudo apt-get install --no-install-recommends unixodbc libsqliteodbc -y

# upgrade pip
RUN sudo -E pip --no-cache-dir install --upgrade pip --ignore-installed
# install uv
RUN sudo -E pip --no-cache-dir install uv
