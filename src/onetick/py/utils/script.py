import os
from pathlib import Path
from warnings import warn

import onetick.py as otp


def __create_bat_script_to_run_config(dir_path, config_path):
    with open(os.path.join(dir_path, "run.bat"), "w") as fout:
        fout.write(f"set ONE_TICK_CONFIG={config_path}\n")

        path = os.path.join(omd_dist_path(), "one_tick", "bin", "OneTickDisplay.exe")

        fout.write(f'start "OneTick GUI" {path} -context DEFAULT\n')


def __create_bat_script_to_run_query_designer(dir_path, config_path):
    with open(os.path.join(dir_path, "run_query_designer.bat"), "w") as fout:
        fout.write(f"set ONE_TICK_CONFIG={config_path}\n")

        path = os.path.join(omd_dist_path(), "one_tick", "bin", "QueryDesigner.exe")

        fout.write(f'start "Query Designer" {path} -context DEFAULT\n')


def write_config_for_tick_server_run(fout, config_path, path):
    fout.write('if [ "$1" == "-" ]; then\n')
    fout.write(
        " " * 4
        + "onetick server --cfg "
        + config_path
        + " -n "
        + os.path.basename(config_path).split(".")[0]
        + " ${@:2}\n"  # this option allows to propagate other custom parameters into the script externally
    )
    fout.write("else\n")
    fout.write(" " * 4 + path + " -context DEFAULT -port $MAIN_TS_PORT > /dev/null 2>&1\n")
    fout.write("fi\n")


def __create_shell_script_to_run_config(dir_path, config_path):
    file_name = os.path.join(dir_path, "run.sh")

    path = os.path.join(omd_dist_path(), "one_tick", "bin", "tick_server.exe")

    with open(file_name, "w") as fout:
        fout.write("#!/bin/bash\n")
        fout.write('if [[ -z "$1" && -z "$MAIN_TS_PORT" ]]; then\n')
        fout.write(
            " " * 4
            + 'echo "Please, define port either using MAIN_TS_PORT environment'
              ' variable or as a first parameter to the ./run.sh script"\n'
        )
        fout.write(" " * 4 + "exit 1\n")
        fout.write("\n")
        fout.write("fi\n")
        fout.write('if [[ -n "$1" ]]; then\n')
        fout.write(" " * 4 + "MAIN_TS_PORT=$1\n")
        fout.write("fi\n")
        fout.write("\n")
        fout.write(f"export ONE_TICK_CONFIG={config_path}\n")
        write_config_for_tick_server_run(fout, config_path, path)

    os.chmod(file_name, 0o750)


def __create_shell_script_to_run_dashboard(dir_path, config_path, main_install_dir):
    if main_install_dir is None:
        return
    file_name = os.path.join(dir_path, "run_dashboard.sh")
    path_ts = os.path.join(omd_dist_path(), "one_tick", "bin", "tick_server.exe")
    client_data_path = os.path.join(main_install_dir, "client_data")
    common_path = os.path.join(main_install_dir, "sol", "common")
    tomcat_cfg = os.path.join(dir_path, 'tomcat.txt')
    db_locator_remote = os.path.join(common_path, "config", "locator.remote")

    Path(tomcat_cfg).write_text('')

    with open(config_path, 'a') as f:
        f.write(f'\nDB_LOCATOR.REMOTE = "{db_locator_remote}"')

    with open(file_name, "w") as fout:
        fout.write("#!/bin/bash\n")
        fout.write(
            'if [[ -z "$1" &&'
            ' -z "$MAIN_TS_PORT" &&'
            ' -z "$CATALINA_HTTP_PORT" &&'
            ' -z "$CATALINA_SERVER_PORT" ]]; then\n')
        fout.write(
            " " * 4
            + 'echo "Please, define ts port to use sequentially ports for catalina '
              'or using \\$MAIN_TS_PORT, \\$CATALINA_HTTP_PORT, '
              '\\$CATALINA_SERVER_PORT environment variable or '
              'as a first parameter to the script"\n'
        )
        fout.write(" " * 4 + "exit 1\n")
        fout.write("\n")
        fout.write("fi\n")
        fout.write('if [[ -n "$1" ]]; then\n')
        fout.write(" " * 4 + "export CATALINA_HTTP_PORT=$1\n")
        fout.write(" " * 4 + "export MAIN_TS_PORT=$(($1 + 1))\n")
        fout.write(" " * 4 + "export CATALINA_SERVER_PORT=$(($1 + 2))\n")
        fout.write("fi\n")
        fout.write("\n")
        fout.write('echo "Running dashboard with CATALINA_HTTP_PORT=$CATALINA_HTTP_PORT, '
                   'MAIN_TS_PORT=$MAIN_TS_PORT, '
                   'CATALINA_SERVER_PORT=${CATALINA_SERVER_PORT}"\n')
        fout.write("export ONE_TICK_CONFIG=" + config_path + "\n")
        fout.write("export MAIN_CLIENT_DIR=" + client_data_path + "\n")
        fout.write("export MAIN_INSTALL_DIR=" + main_install_dir + "\n")
        fout.write("export OMD_WEB_DASHBOARD_CONFIG_FILE="
                   + os.path.join(client_data_path, "config", "web_dashboard.cfg") + "\n")
        fout.write("export COMMON_PACK=" + common_path + "\n")
        fout.write("export SURV_PACK=" + os.path.join(main_install_dir,
                                                      "sol", "surveillance") + "\n")
        fout.write("export BESTEX_PACK=" + os.path.join(main_install_dir,
                                                        "sol", "bestex") + "\n")
        fout.write("export WORKFLOW_PACK=" + os.path.join(main_install_dir,
                                                          "sol", "workflow") + "\n")
        fout.write("source " + os.path.join(common_path, "config", "src",
                                            "env_common_functions.sh") + "\n")

        fout.write('if isAWSHosted\n')
        fout.write('then\n')
        fout.write('  export_if_empty MAIN_ONE_TICK_DIR="/opt/one_market_data/one_tick"\n')
        fout.write('  export_if_empty LICENSE_DIR="/license"\n')
        fout.write('  export_if_empty VENDOR_DATA_DIR="${LOCATOR_LOCAL_DATA_DIR}/vendor_data/"\n')
        fout.write('  export_if_empty JAVA_HOME="/usr/bin/java"\n')
        fout.write('\n')
        fout.write('  if [[ -d /usr/share/tomcat8 ]]\n')
        fout.write('  then\n')
        fout.write('    export_if_empty CATALINA_HOME="/usr/share/tomcat8"\n')
        fout.write('  else\n')
        fout.write('    export_if_empty CATALINA_HOME="/usr/share/tomcat"\n')
        fout.write('  fi\n')
        fout.write('\n')
        fout.write('  # trying system tomcat, then client tomcat, then sol tomcat\n')
        fout.write('  if [[ -d "/etc/omd/tomcat" ]];  then\n')
        fout.write('    export_if_empty CATALINA_BASE="/etc/omd/tomcat"\n')
        fout.write('  elif [[ -d "${MAIN_CLIENT_DIR}/config/tomcat" ]]; then\n')
        fout.write('    export_if_empty CATALINA_BASE="${MAIN_CLIENT_DIR}/config/tomcat"\n')
        fout.write('  else\n')
        fout.write('    export_if_empty CATALINA_BASE="${COMMON_PACK}/tomcat"\n')
        fout.write('  fi\n')
        fout.write('fi\n')
        fout.write('\n')
        fout.write('if isDeployed\n')
        fout.write('then\n')
        fout.write('  export_if_empty MAIN_TS_PORT=12345\n')
        fout.write('  export_if_empty CATALINA_SERVER_PORT=12346\n')
        fout.write('  export_if_empty CATALINA_HTTP_PORT=8080\n')
        fout.write('  export_if_empty OMD_WEB_DASHBOARD_CONFIG_FILE=${MAIN_CLIENT_DIR}/config/web_dashboard.cfg\n')
        fout.write('  export_if_empty MAIN_ONE_TICK_DIR="$MAIN_INSTALL_DIR/../onetick/one_market_data/one_tick"\n')
        fout.write('  export_if_empty LICENSE_DIR="$MAIN_INSTALL_DIR/../license"\n')
        fout.write('  export_if_empty JAVA_HOME="$MAIN_INSTALL_DIR/../java/jdk1.8.0_202"\n')
        fout.write('  export_if_empty CATALINA_HOME="$MAIN_INSTALL_DIR/../tomcat"\n')
        fout.write('  export_if_empty CATALINA_BASE="$MAIN_CLIENT_DIR/config/tomcat"\n')
        fout.write('fi\n')
        fout.write('export_if_empty CATALINA_PID="$CATALINA_BASE/bin/pid"\n')
        fout.write('export_if_empty CATALINA_TMPDIR="$CATALINA_BASE/temp"\n')
        fout.write(f'export TOMCAT_CFG="{tomcat_cfg}"\n')
        fout.write(
            'export JAVA_OPTS="-Djava.awt.headless=true -Dfile.encoding=UTF-8 '
            '-server -Xms512m -Xmx512m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC '
            '-XX:+AggressiveOpts -DServerPort=$CATALINA_SERVER_PORT $JAVA_OPTS"\n')
        fout.write(
            'export CATALINA_OPTS="-Djna.tmpdir=$CATALINA_TMPDIR '
            '-Dcatalina.log.path=$MAIN_LOG_DIR -DHttpPort=$CATALINA_HTTP_PORT '
            '-DHttpsPort=$CATALINA_HTTPS_PORT -DUser=$USER $CATALINA_OPTS"\n')
        fout.write(
            'export PATH="$MAIN_ONE_TICK_DIR/bin:$JAVA_HOME/bin:$CATALINA_HOME/bin:$COMMON_PACK/scripts:$PATH"\n')
        fout.write(
            'export LD_LIBRARY_PATH="$MAIN_ONE_TICK_DIR/bin:'
            '$MAIN_ONE_TICK_DIR/lib:$MAIN_ONE_TICK_DIR/bin/wombat:'
            '$MAIN_ONE_TICK_DIR/bin/activ:$MAIN_ONE_TICK_DIR/bin/rfa:'
            '$MAIN_ONE_TICK_DIR/bin/spryware:$LD_LIBRARY_PATH"\n')
        fout.write('export_if_empty MAIN_TS_IP="localhost"\n')
        fout.write('if isAWSHosted\n')
        fout.write('then\n')
        fout.write('  ' + os.path.join(common_path, "scripts",
                                       "impl", "start_apache_aws.sh") + "\n")
        fout.write('fi\n')
        fout.write('if isDeployed\n')
        fout.write('then\n')
        fout.write('  ' + os.path.join(common_path, "scripts",
                                       "impl", "start_apache.sh") + "\n")
        fout.write('fi\n')
        write_config_for_tick_server_run(fout, config_path, path_ts)
        fout.write('if isAWSHosted\n')
        fout.write('then\n')
        fout.write('  ' + os.path.join(common_path, "scripts",
                                       "impl", "stop_apache_aws.sh") + "\n")
        fout.write('fi\n')
        fout.write('if isDeployed\n')
        fout.write('then\n')
        fout.write('  ' + os.path.join(common_path, "scripts",
                                       "impl", "stop_apache.sh") + "\n")
        fout.write('fi\n')

    os.chmod(file_name, 0o750)


def create_file_to_run_config(dir_path, config_path, main_install_dir=None):
    if otp.__webapi__:
        warn("OTP_WEBAPI is set, means local configs will not work with remote OneTick server")
    if os.name == "nt":
        __create_bat_script_to_run_config(dir_path, config_path)
        __create_bat_script_to_run_query_designer(dir_path, config_path)
    else:
        __create_shell_script_to_run_config(dir_path, config_path)
        __create_shell_script_to_run_dashboard(dir_path, config_path,
                                               main_install_dir)


def omd_dist_path():
    """
    Returns path to the OneTick distribution calculated from the
    PYTHONPATH or MAIN_ONE_TICK_DIR. If path is not found, then FileNotFoundError is risen.
    Example for a linux system: /opt/one_market_data
    """
    omd_prefix = Path('one_market_data/one_tick')
    res_path = None

    paths = []
    pythonpath = os.environ.get('PYTHONPATH')
    if pythonpath:
        paths = pythonpath.split(os.pathsep)
    main_one_tick_dir = os.environ.get('MAIN_ONE_TICK_DIR')
    if main_one_tick_dir:
        paths.append(main_one_tick_dir)

    for value in paths:
        if str(omd_prefix) in str(Path(value)):
            res_path = value
            break

    if res_path is None:
        raise FileNotFoundError('Path to OneTick distribution is not found')

    base_prefix = 'one_market_data'
    omd_prefix_pos = res_path.find(base_prefix)
    res_path = res_path[: omd_prefix_pos + len(base_prefix)]

    return res_path
