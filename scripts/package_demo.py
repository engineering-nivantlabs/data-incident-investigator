"""Package the runnable local demonstration; never include credentials or local runtime data."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root=Path(__file__).resolve().parent.parent
paths=[root/p for p in ['README.md','requirements.txt','app.yaml','databricks.yml','.gitignore']]
for folder in ['delivery','investigator','static','config','scripts','tests','docs/hackathon']:
    paths.extend(p for p in (root/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix in {'.py','.json','.html','.js','.css','.cjs','.sql','.md','.png'})
paths.extend(root/'docs'/name for name in ['databricks-pilot.md','competitive-landscape.md'])
target=root/'deliveryguard-hackathon-demo.zip'
staging=target.with_suffix('.zip.tmp')
with ZipFile(staging,'w',ZIP_DEFLATED) as archive:
    for path in sorted(set(paths)):
        archive.write(path,Path('deliveryguard-hackathon-demo')/path.relative_to(root))
with ZipFile(staging) as archive:
    assert archive.testzip() is None
    for path in set(paths):
        assert archive.read(str(Path('deliveryguard-hackathon-demo')/path.relative_to(root)))==path.read_bytes()
staging.replace(target)
print(f'Verified {len(set(paths))} files: {target}')
