"""Kill only the Breakwater worker, restart it, and assert lease recovery."""
import subprocess


def run(*args):
    subprocess.run(['docker','compose',*args],check=True)


run('exec','-T','api','python','tests/recovery_probe.py','prepare')
try:
    run('kill','-s','SIGKILL','worker')
finally:
    run('start','worker')
run('exec','-T','api','python','tests/recovery_probe.py','verify')
