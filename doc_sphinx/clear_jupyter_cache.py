from collections import defaultdict
from pathlib import Path

import jupyter_cache

DOC_SPHINX_DIR = Path(__file__).parent.absolute()

cache = jupyter_cache.get_cache(DOC_SPHINX_DIR / '.jupyter_cache')
records = cache.list_cache_records()

paths_dict = defaultdict(list)
for record in records:
    record = record.to_dict()
    path = record['uri'].removeprefix(str(DOC_SPHINX_DIR))
    paths_dict[path].append((record['pk'], record['created']))

for path, tuple_records in paths_dict.items():
    if len(tuple_records) > 1:
        sorted_records = sorted(tuple_records, key=lambda r: r[1])
        new_id = sorted_records[-1][0]
        old_ids = [r[0] for r in sorted_records[:-1]]
        print(f'Found old records for {path} (new id {new_id}): deleted {old_ids}')
        for old_id in old_ids:
            cache.remove_cache(old_id)
