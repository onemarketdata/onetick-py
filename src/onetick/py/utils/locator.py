import os
from locator_parser.io import PrintWriter, FileReader
from locator_parser.actions import GetAll
from locator_parser.locator import Location, parse_locator
from locator_parser.common import apply_actions

from .temp import TmpFile
from .types import default


def get_dbs_locations_from_locator(locator):
    action = GetAll()
    action.add_where(Location)

    apply_actions(parse_locator, FileReader(locator), PrintWriter(), [action])

    return map(lambda x: x.location, action.result)


def tmp_locator(clean_up=default, empty=False):
    import onetick.py as otp

    STUBS = {'COMMON'}
    default_db = otp.config.get('default_db')
    if default_db:
        STUBS.add(default_db)

    data = []
    data.append('<VERSION_INFO VERSION="2"/>')
    data.append("<DATABASES>")
    data.append("")

    if not empty:
        for stub_name in STUBS:
            data.append(
                f'<db ID="{stub_name}" symbology="{otp.config.default_symbology}" time_series_is_composite="YES">'
            )
            data.append("<locations>")
            day_boundary_tz = otp.config.get('tz')
            day_boundary_tz = f'day_boundary_tz="{day_boundary_tz}"' if day_boundary_tz else ''
            data.append(
                '<location access_method="file" location="/opt/one_market_data/one_tick/examples/data/demo_level1" '
                'start_time="20001201000000" end_time="20301031050000" '
                f'{day_boundary_tz}/>'
            )
            data.append("</locations>")
            data.append('<feed type="heartbeat_generator">')
            data.append('<options format="native" />')
            data.append('</feed>')
            data.append("</db>")
            data.append("")
            data.append("")

    data.append("</DATABASES>")

    data.append("<TICK_SERVERS>")
    data.append("</TICK_SERVERS>")
    data.append("<CEP_TICK_SERVERS>")
    data.append("</CEP_TICK_SERVERS>")
    data.append("<INCLUDES>")
    data.append("</INCLUDES>")

    tmp_file = new_tmp_locator(clean_up)

    with open(tmp_file, "w") as fout:
        fout.write("\n".join(data))

    return tmp_file


def new_tmp_locator(clean_up):
    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        tmp_file = TmpFile(name="locator.default", clean_up=clean_up, force=True)
    else:
        tmp_file = TmpFile(suffix=".locator", clean_up=clean_up)
    return tmp_file


def empty_locator(clean_up=default):
    data = []
    data.append('<VERSION_INFO VERSION="2"/>')
    data.append("<DATABASES>")
    data.append("</DATABASES>")
    data.append("<TICK_SERVERS>")
    data.append("</TICK_SERVERS>")
    data.append("<CEP_TICK_SERVERS>")
    data.append("</CEP_TICK_SERVERS>")
    data.append("<INCLUDES>")
    data.append("</INCLUDES>")

    tmp_file = new_tmp_locator(clean_up)
    with open(tmp_file, "w") as fout:
        fout.write("\n".join(data))
    return tmp_file
