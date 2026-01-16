# Manual script to sync version in dbt_project.yml and README.md with CHANGELOG.md
import re
from pathlib import Path

root = Path(__file__).parent.parent
version = re.search(r'^## (\d+\.\d+\.\d+)', (root / 'CHANGELOG.md').read_text(), re.M).group(1)

dbt_file = root / 'dbt_project.yml'
dbt_file.write_text(re.sub(r"version: '[^']+'", f"version: '{version}'", dbt_file.read_text()))

readme_file = root / 'README.md'
readme_text = re.sub(r'revision: v[\d.]+', f'revision: v{version}', readme_file.read_text())
readme_file.write_text(re.sub(r'tags/[\d.]+\.tar\.gz', f'tags/{version}.tar.gz', readme_text))
