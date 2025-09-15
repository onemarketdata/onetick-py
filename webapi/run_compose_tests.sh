# Description: Run the tests in the corresponding container
docker-compose exec otpwebapi sudo -E -u onetick pytest -m 'not (integration or performance)' --color=no tests/core/ > test-compose-20240530-build-updated.log
