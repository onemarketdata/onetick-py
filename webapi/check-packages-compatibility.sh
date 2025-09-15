set -e  # exit on error
set -x  # show all commands

# otp is always installed in site-packages (we do not check distributed version here)
cd /onetick-py/

# prepare requirements for webapi
sudo pip install -U pip
sudo pip install "/onetick-py/[webapi]" --extra-index-url https://pip.sol.onetick.com -q --root-user-action=ignore
sudo pip uninstall -y onetick.query_webapi -q --root-user-action=ignore

# revert previous run, useful for development
sudo mv /opt.bak /opt | true

echo -e "\n1) Simple check for original onetick.query and onetick.py compatibility"
unset OTP_WEBAPI
export MAIN_ONE_TICK_DIR="/opt/one_market_data/one_tick"
python3 -c "import onetick.py as otp; assert otp.otq.otq.webapi == False"
# python3 /onetick-py/onetick/tests/test_otp.py

echo -e "\n2) webapi pip installed, OTP_WEBAPI is unset, checking that onetick.query is used"
sudo pip install "/onetick-py/[webapi]" --extra-index-url https://pip.sol.onetick.com -q --root-user-action=ignore
unset OTP_WEBAPI
python3 -c "import onetick.py as otp; assert otp.otq.otq.webapi == False"
# python3 /onetick-py/onetick/tests/test_otp.py

echo -e "\n3) webapi pip installed, OTP_WEBAPI=1, checking that onetick.query_webapi is used"
export OTP_WEBAPI=1
python3 -c "import onetick.py as otp; assert otp.otq.otq.webapi == True"
# python3 /onetick-py/onetick/tests/test_otp.py

echo -e "\n4) webapi is in dist, OTP_WEBAPI is unset, checking that automatically selected onetick.query"
sudo pip uninstall  -y onetick.query_webapi
unset OTP_WEBAPI
python3 -c "import onetick.py as otp; assert otp.otq.otq.webapi == False"
# python3 /onetick-py/onetick/tests/test_otp.py

# # but current build has onetick.query_webapi not compatible with onetick.py, temporary skipping until new release
# echo -e "\n5) webapi is in dist, OTP_WEBAPI is set to 1, checking that it is forced to onetick.query_webapi "
# export OTP_WEBAPI=1
# python3 -c "import onetick.py as otp; assert otp.otq.otq.webapi == True"

echo -e "\n6) webapi pip installed, OTP_WEBAPI is unset, onetick.query removed from dist, checking that automatically selected onetick.query_webapi"
sudo pip install "/onetick-py/[webapi]" --extra-index-url https://pip.sol.onetick.com -q --root-user-action=ignore
unset OTP_WEBAPI
sudo mv /opt /opt.bak
python3 -c "import onetick.py as otp; assert otp.otq.otq.webapi == True"
# python3 /onetick-py/onetick/tests/test_otp.py
