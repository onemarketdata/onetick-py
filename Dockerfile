ARG ONETICK_BUILD

# image is based on the ${ONETICK_BUILD} image
FROM ${ONETICK_BUILD}

ARG LOCAL_PIP_URL

# can be used to install fixed pandas+numpy version,
# as onetick-py has flexible requirements
ARG STRICT_DEPENDENCIES

# gitlab checkouts code into the current directory, ie .
# and this command copy code inside the image
# (we also change ownership of the directory to onetick user,
#  because tests and build scripts are run by onetick user and the ownership is required by git)
COPY --chown=onetick:root --chmod=775 . /onetick-py/

RUN sudo apt-get update
RUN sudo apt-get install --no-install-recommends graphviz -y
RUN sudo apt-get install --no-install-recommends enchant-2 -y
RUN sudo apt-get install --no-install-recommends zip -y
RUN sudo apt-get install --no-install-recommends unixodbc libsqliteodbc -y

# upgrade pip
RUN sudo -E pip --no-cache-dir install --upgrade pip --ignore-installed
# install uv
RUN sudo -E pip --no-cache-dir install uv

# install onetick-py with development dependencies
RUN sudo -E uv pip --no-cache-dir install --system --group "/onetick-py/pyproject.toml:dev" \
                                          --extra-index-url "https://${LOCAL_PIP_URL}" \
                                          -e "/onetick-py[${STRICT_DEPENDENCIES}]"
