import json
import re
import subprocess

if __name__ == '__main__':
    releases = subprocess.run(['git', 'tag', '--list'],
                              capture_output=True,
                              text=True).stdout.strip().split('\n')

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
