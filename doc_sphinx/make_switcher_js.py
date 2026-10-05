import json
import re
import subprocess

from pathlib import Path

OTP_DIR = Path(__file__).parent.parent

if __name__ == '__main__':
    # this script is run by onetick user but the ownership is required by git
    subprocess.check_output(f'git config --global --add safe.directory {OTP_DIR}', shell=True)
    print(subprocess.check_output('git config --global --list', shell=True))
    result = subprocess.run(['git', 'tag', '--list'], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)

    releases = result.stdout.strip().split('\n')

    # filter out only x.x.x style tags by regex
    releases = [x for x in releases if re.match(r'^\d+\.\d+\.\d+$', x)]

    # sort by version number
    releases = sorted(releases, key=lambda x: tuple(map(int, x.split('.'))))

    output = [{
        'name': releases[-1],
        'url': '/',
        'version': releases[-1],
    }]
    for version in reversed(releases):
        if version == releases[-1]:
            continue
        output.append({
            'name': version,
            'url': '/release/' + version + '/',
            'version': version,
        })

    with open('js/switcher.json', 'w') as fp:
        json.dump(output, fp, indent=4)
