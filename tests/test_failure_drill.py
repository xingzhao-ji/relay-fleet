import asyncio
from scripts.experiment import drill

def test_live_socket_cut_sigkill_replay_revocation_and_config(tmp_path):
    report=asyncio.run(drill(tmp_path/'runtime',tmp_path/'evidence'))
    assert report['passed']
    assert all(check['passed'] for check in report['checks'].values())
