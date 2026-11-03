"""Build an allowlisted program-only package for the LMPC update executor."""
import argparse, ast, hashlib, io, json, pathlib, tarfile

FILES=['channel_manager.py','measure_gain.py','migrate_from_tvlink.py','requirements.txt','README.md','LICENSE.md','LICENSE-SCOPE.md']
def build(output):
    root=pathlib.Path(__file__).resolve().parent;tree=ast.parse((root/'channel_manager.py').read_text(encoding='utf-8'))
    version=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='__version__' for t in n.targets))
    output=pathlib.Path(output);output.mkdir(parents=True,exist_ok=True);package=output/'channel-manager.tar.gz'
    with tarfile.open(package,'w:gz') as tf:
        data=json.dumps({'component':'channel-manager','version':version}).encode();m=tarfile.TarInfo('manifest.json');m.size=len(data);tf.addfile(m,io.BytesIO(data))
        for name in FILES:tf.add(root/name,arcname='payload/'+name,recursive=False)
    digest=hashlib.sha256(package.read_bytes()).hexdigest();(output/'SHA256SUMS').write_text(digest+'  '+package.name+'\n')
    print(json.dumps({'version':version,'file':str(package),'sha256':digest}));return version

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',default='dist');a=p.parse_args();build(a.output)
