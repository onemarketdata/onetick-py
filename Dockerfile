ARG ONETICK_BUILD

# image is based on the ${ONETICK_BUILD} image
FROM ${ONETICK_BUILD}

ARG LOCAL_PIP_URL

# can be used to install fixed pandas+numpy version,
# as onetick-py has flexible requirements
ARG STRICT_DEPENDENCIES

# gitlab checkouts code into the current directory, ie .
# and this command copy code inside the image
COPY . /onetick-py/

RUN sudo apt-get update
RUN sudo apt-get install --no-install-recommends graphviz -y
RUN sudo apt-get install --no-install-recommends enchant-2 -y
RUN sudo apt-get install --no-install-recommends zip -y
RUN sudo apt-get install --no-install-recommends unixodbc libsqliteodbc -y

# install python dependenices based on the requirements.dev.txt file
RUN sudo -E pip --no-cache-dir install --upgrade pip --ignore-installed \
    && sudo -E pip --no-cache-dir install -r "/onetick-py/requirements.dev.txt" \
       --extra-index-url "https://${LOCAL_PIP_URL}" \
    && [ -n "${STRICT_DEPENDENCIES}" ] && pip install -U /onetick-py/[strict] || pip install -U /onetick-py/
