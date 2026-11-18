#!/usr/bin/env python3
"""Build the downloadable lesson snapshots from the tracked source trees."""
from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SKIP = {'.git', '.tools', '.bin', '__pycache__', '.DS_Store', 'local.env', 'certs'}


def files(directory: Path) -> list[Path]:
    return sorted(p for p in directory.rglob('*') if p.is_file() and not SKIP.intersection(p.relative_to(directory).parts) and not p.name.endswith('.log'))


def build(manifest: dict[str, Any]) -> None:
    output = ROOT / 'dist'
    output.mkdir(exist_ok=True)
    checksums: list[str] = []
    for archive in manifest['archives']:
        with tempfile.TemporaryDirectory(prefix='go-microservices-package-') as temporary:
            source = ROOT / archive['source']
            if archive['lesson'] == 'microservice-testing':
                generated = Path(temporary) / 'examples'
                shutil.copytree(source, generated)
                for name, lesson in manifest['test_bundle'].items():
                    shutil.copytree(ROOT / 'examples' / lesson / 'example', generated / name, ignore=shutil.ignore_patterns('.tools', '.bin', '*.log'))
                source = generated
            destination = output / archive['file']
            if archive['format'] == 'zip':
                with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as target:
                    for path in files(source):
                        target.write(path, str(Path(archive['prefix']) / path.relative_to(source)))
            else:
                with tarfile.open(destination, 'w:gz') as target:
                    for path in files(source):
                        target.add(path, arcname=str(Path(archive['prefix']) / path.relative_to(source)), recursive=False)
            digest = hashlib.sha256(destination.read_bytes()).hexdigest()
            checksums.append(f'{digest}  {archive["file"]}')
    (output / 'SHA256SUMS').write_text('\n'.join(checksums) + '\n')
    print(f'Built {len(checksums)} archives in {output}')


if __name__ == '__main__':
    build(json.loads((ROOT / 'archives.json').read_text()))
