#!/usr/bin/env python3
"""Verify the reconciled source against the captured deployment, without networking."""
import argparse
import ast
import hashlib
import json
from pathlib import Path, PurePosixPath
import shlex

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = Path(__file__).with_name('manifest.json')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copied_files(recipe):
    """Project source COPYs and the one explicit exclusion; do not execute a build."""
    result = {}
    for line in recipe.read_text().splitlines():
        if line.startswith('COPY '):
            parts = shlex.split(line)
            assert len(parts) == 3 and not parts[1].startswith('--'), line
            source, dest = ROOT / parts[1], PurePosixPath(parts[2])
            if not dest.is_absolute():
                dest = PurePosixPath('/app') / dest
            if source.is_dir():
                for path in source.rglob('*'):
                    if path.is_file():
                        result[str(dest / path.relative_to(source).as_posix())] = path
            else:
                if parts[2] == '.' or parts[2].endswith('/'):
                    dest /= source.name
                result[str(dest)] = source
        elif line == 'RUN rm /shared/shared/privacy.py':
            del result['/shared/shared/privacy.py']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, action='append', default=[],
                        help='Optional private live-inventory JSON; never committed.')
    args = parser.parse_args()
    manifest = json.loads(MANIFEST.read_text())
    observed = {}
    for path in args.inventory:
        observed.update(json.loads(path.read_text())['containers'])
    count = 0
    for name, component in manifest['components'].items():
        if observed:
            live = observed[name]
            assert live['image_id'] == component['image_id'], name
            assert set(live.get('files', {})) == set(component['files']), name
        if name == 'addax-pilot-frontend':
            if observed:
                assets = {line.split('  ', 1)[1]: line.split('  ', 1)[0]
                          for line in observed[name]['static_hashes'].splitlines()}
                assert assets == component['asset_hashes']
            continue
        service = {'addax-pilot-api': 'api', 'addax-pilot-bulk': 'bulk-upload',
                   'addax-pilot-detection': 'detection',
                   'addax-pilot-speciesnet': 'classification-speciesnet'}[name]
        recipe = ROOT / manifest['build_profiles'].get(service, 'services/api/Dockerfile')
        effective = copied_files(recipe)
        for destination, entry in component['files'].items():
            path = ROOT / entry['source']
            assert digest(path) == entry['sha256'], str(path)
            assert destination in effective, destination
            assert digest(effective[destination]) == entry['sha256'], destination
            if observed:
                assert observed[name]['files'][destination]['sha256'] == entry['sha256']
            if path.suffix == '.py':
                ast.parse(path.read_bytes(), filename=str(path))
            count += 1
        # A source profile must not silently add Python modules under the shared library.
        actual_shared = {p for p in effective if p.startswith('/shared/') and p.endswith('.py')}
        expected_shared = {p for p in component['files'] if p.startswith('/shared/') and p.endswith('.py')}
        assert actual_shared == expected_shared, name
    revisions, parents = set(), set()
    for path in (ROOT / 'services/api/alembic/versions').glob('*.py'):
        values = {}
        for node in ast.parse(path.read_bytes()).body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in {'revision', 'down_revision'}:
                        values[target.id] = ast.literal_eval(node.value)
        if 'revision' not in values:
            continue
        revisions.add(values['revision'])
        parent = values.get('down_revision')
        parents.update(parent if isinstance(parent, (tuple, list)) else [parent] if parent else [])
    assert parents <= revisions
    assert revisions - parents == {'20261014_preserve_archive_merge'}
    print(json.dumps({'verified_source_files': count, 'components': len(manifest['components']),
                      'source_copy_profiles': 'passed', 'migration_head': '20261014_preserve_archive_merge',
                      'live_inventory_comparison': bool(observed), 'image_rebuild_performed': False}))


if __name__ == '__main__':
    main()
