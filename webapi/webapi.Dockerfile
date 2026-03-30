ARG ONETICK_QUERY_WEBAPI_PYTHON_VERSION
FROM 977320806745.dkr.ecr.us-east-1.amazonaws.com/service/python:$ONETICK_QUERY_WEBAPI_PYTHON_VERSION

ARG LOCAL_PIP_URL
ARG ONETICK_QUERY_WEBAPI_VERSION

# Libraries for correct OneTick functioning
ENV BUILD_PACKAGES "libgomp1 gcc libncurses5-dev sudo sssd vim less libodbc2 jq"

ENV DEBIAN_FTP "https://ftp.debian.org/debian/pool/main"

RUN echo "Building WebAPI client image for version $ONETICK_QUERY_WEBAPI_VERSION"

# Install libraries specified in BUILD_PACKAGES and sentry-cli
RUN apt-get update \
    && apt-get install -y $BUILD_PACKAGES graphviz \
    && curl -sL https://sentry.io/get-cli/ | bash \
    && rm -rf /var/lib/apt/lists/*

# create the onetick user
# add onetick user to the 'dev' group
# add 'dev' group
RUN useradd -m -s /bin/bash -g 0 onetick \
    && groupadd dev  \
    && usermod -aG sudo onetick \
    && usermod -aG dev onetick  \
    && echo "onetick ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && mkdir -p /product/log/airflow /product/pids /product/data /product/otq_cache_dir /product/csv_cache_dir /product/tmp/otqs /product/tomcat \
    && chown -R onetick /product/ \
    && ln -s /usr/lib/x86_64-linux-gnu /usr/lib64

# gitlab checkouts code into the current directory, ie .
# and this command copy code inside the image
RUN sudo apt-get update
RUN sudo apt-get install --no-install-recommends graphviz -y
RUN sudo apt-get install --no-install-recommends enchant-2 -y
RUN sudo apt-get install --no-install-recommends zip -y
RUN sudo apt-get install --no-install-recommends unixodbc libsqliteodbc -y

# upgrade pip
RUN sudo -E pip --no-cache-dir install --upgrade pip --ignore-installed
# install uv
RUN sudo -E pip --no-cache-dir install uv

# install onetick-py from current directory with development dependencies
RUN --mount=type=bind,source=.,target=/onetick-py,rw \
    sudo -E uv pip --no-cache-dir install --system \
                                          --extra-index-url "https://${LOCAL_PIP_URL}" \
                                          --group "/onetick-py/pyproject.toml:dev" \
                                          -e "/onetick-py"

# install onetick.query_webapi
RUN sudo -E uv pip install --system onetick.query_webapi==${ONETICK_QUERY_WEBAPI_VERSION} \
                           --extra-index-url "https://${LOCAL_PIP_URL}"

# needed to fix the problem with urllib3==2.6.0
RUN sudo -E uv pip install --system "backports.zstd; python_version < '3.14'"

ENV OTP_WEBAPI=1
ENV OTP_OTQ_DEBUG_MODE=1
ENV OTP_WEBAPI_TEST_MODE=1

COPY ./webapi/initial_configs /initial_configs

WORKDIR /onetick-py/
