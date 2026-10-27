"""Create a minimal upload folder without local telemetry, credentials or dependencies."""
import shutil
from pathlib import Path

root = Path(__file__).resolve().parent.parent
target = root/'pilot-source'
# Refuse overwrite: stale files or secrets should never silently enter the package.
target.mkdir(exist_ok=False)
for name in ['app.yaml', 'requirements.txt']:
    shutil.copy2(root/name, target/name)
for name in ['investigator', 'static']:
    shutil.copytree(root/name, target/name, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
(target/'config').mkdir()
shutil.copy2(root/'config/pilot.json', target/'config/pilot.json')
print(f'Pilot upload folder: {target}')
