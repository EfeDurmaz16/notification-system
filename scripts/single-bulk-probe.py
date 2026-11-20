#!/usr/bin/env python3
"""Measure standalone and bulk low SMS while a same-priority campaign drains."""
import argparse
import concurrent.futures
import datetime
import json
import pathlib
import runpy
import time

helpers = runpy.run_path(str(pathlib.Path(__file__).with_name('urgent-probe.py')))
http, sql, item, wait_for, sql_ids, command = (
    helpers[key] for key in ('http', 'sql', 'item', 'wait_for', 'sql_ids', 'command')
)
BASELINE_API = helpers['API']
PROVIDER = helpers['PROVIDER']


def experiment(trial, order, probe_api):
    assert sql("SELECT count(*) FROM notifications WHERE status IN ('pending','sending','retry_wait')") == '0'
    idle = http(probe_api + '/v1/notifications', item('low', 'Idle single control'))['id']
    wait_for(lambda: http(probe_api + '/v1/notifications/' + idle)['status'] == 'accepted')
    time.sleep(1.1)
    before = http(PROVIDER + '/stats')['attempts']

    def create_batch(n):
        items = [item('low', f'Campaign {trial}/{n}/{i}') for i in range(1000)]
        response = http(BASELINE_API + '/v1/batches', {'notifications': items})
        assert len(response['items']) == 1000 and all(entry.get('id') for entry in response['items'])
        return response['batchId']

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        campaign = list(pool.map(create_batch, range(10)))
    wait_for(lambda: http(PROVIDER + '/stats')['attempts'] - before >= 500)
    probes = {'idle': idle}
    payload = item('low', 'Same-priority probe')
    for route in order:
        if route == 'single':
            probes[route] = http(probe_api + '/v1/notifications', payload)['id']
        else:
            probes[route] = http(probe_api + '/v1/batches', {'notifications': [payload]})['items'][0]['id']
    print(f'Trial {trial}: probes submitted ({order}); waiting for campaign.', flush=True)
    campaign_filter = 'batch_id IN (' + sql_ids(campaign) + ')'
    wait_for(lambda: http(PROVIDER + '/stats')['attempts'] - before >= 10002)
    wait_for(lambda: sql("SELECT count(*) FROM notifications WHERE status='accepted' AND " + campaign_filter) == '10000')
    for identifier in probes.values():
        wait_for(lambda: http(probe_api + '/v1/notifications/' + identifier)['status'] == 'accepted')
    rows = json.loads(sql(f"""
       WITH c AS (SELECT created_at,updated_at FROM notifications WHERE {campaign_filter}),
       p AS (SELECT * FROM notifications WHERE id IN ({sql_ids(probes.values())}))
       SELECT json_build_object('campaignDrainSec',(SELECT extract(epoch FROM max(updated_at)-min(created_at)) FROM c),
       'campaignAccepted',(SELECT count(*) FROM c),
       'ids',(SELECT json_object_agg(id,json_build_object('createdAt',created_at,'acceptedAt',updated_at,
         'queueMs',1000*extract(epoch FROM first_attempt_at-created_at),
         'sendMs',1000*extract(epoch FROM updated_at-first_attempt_at),
         'totalMs',1000*extract(epoch FROM updated_at-created_at),
         'campaignOutstandingAtCreate',(SELECT count(*) FROM c WHERE created_at<=p.created_at AND updated_at>p.created_at),
         'campaignStillOutstandingAtAcceptance',(SELECT count(*) FROM c WHERE created_at<=p.updated_at AND updated_at>p.updated_at))) FROM p))
    """))
    rows['probes'] = {}
    for key, identifier in probes.items():
        attempts = http(PROVIDER + '/stats/' + identifier)['attempts']
        assert attempts == 1
        rows['probes'][key] = dict(rows['ids'][identifier], id=identifier, providerPosts=attempts)
    del rows['ids']
    rows['loadedProviderPosts'] = http(PROVIDER + '/stats')['attempts'] - before
    assert rows['loadedProviderPosts'] == 10002 and rows['campaignAccepted'] == 10000
    rows['order'] = order
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variant', choices=('baseline', 'candidate'), required=True)
    parser.add_argument('--trial', type=int, choices=(1, 2), default=1)
    args = parser.parse_args()
    probe_api = BASELINE_API if args.variant == 'baseline' else 'http://127.0.0.1:18081'
    command('scripts/require-local-provider.sh')
    workers = command('docker', 'compose', 'ps', '--status', 'running', '-q', 'worker').splitlines()
    assert len(workers) == 3
    report = {
        'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'revision': command('git', 'rev-parse', 'HEAD'),
        'workerReplicas': len(workers),
        'variant': args.variant,
        'api': probe_api,
        'workerImage': command('docker', 'inspect', '--format', '{{.Image}}', workers[0]),
        'trials': [],
    }
    output = helpers['ROOT'] / 'reports' / 'ingress-probe' / f'{args.variant}-{args.trial}.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    order = ['single', 'bulk'] if args.trial == 1 else ['bulk', 'single']
    result = experiment(args.trial, order, probe_api)
    report['trials'].append(result)
    output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(result), flush=True)
