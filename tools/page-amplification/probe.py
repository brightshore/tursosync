"""Opt-in pull page-amplification probe; writes only to its own disposable database."""
import argparse
import collections
import gzip
import io
import json
import pathlib
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager

MAX_RESPONSE = 16 * 1024 * 1024

def varint(value):
    output=bytearray()
    while value>127:
        output.append((value&127)|128); value >>= 7
    output.append(value)
    return bytes(output)

def decode_varint(data, offset=0):
    value=0; shift=0
    while True:
        assert offset < len(data) and shift < 70, 'Truncated or oversized protobuf varint'
        b=data[offset]; offset+=1; value |= (b&127)<<shift
        if b<128: return value,offset
        shift+=7

def fields(data):
    output={}; offset=0
    while offset<len(data):
        tag,offset=decode_varint(data,offset); key=tag>>3; wire=tag&7
        if wire==0: value,offset=decode_varint(data,offset)
        elif wire==2:
            length,offset=decode_varint(data,offset)
            assert offset + length <= len(data), 'Truncated protobuf field'
            value=data[offset:offset+length]; offset+=length
        else: raise ValueError('Unexpected protobuf wire type')
        output[key]=value
    return output

def read_header(stream):
    prefix=bytearray()
    while True:
        b=stream.read(1)
        if not b:
            assert not prefix, 'Truncated message prefix'
            return None
        prefix+=b
        if b[0]<128: break
        assert len(prefix)<10
    length,_=decode_varint(prefix); assert length<131072
    data = stream.read(length)
    assert len(data) == length, 'Truncated message'
    return fields(data)

class Endpoint:
    def __init__(self, url, token=None):
        self.url = url.rstrip('/').replace('libsql://', 'https://')
        self.headers = {'Accept-Encoding': 'identity', 'User-Agent': 'tursosync-controlled-page-probe'}
        if token: self.headers['Authorization'] = 'Bearer '+token

    def sql(self, *queries):
        body = json.dumps({'requests': [{'type': 'execute', 'stmt': {'sql': sql, 'args': [], 'want_rows': True}} for sql in queries]}).encode()
        request = urllib.request.Request(self.url+'/v2/pipeline', data=body, headers={**self.headers, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=30) as response:
            results = json.load(response)['results']
        for result in results:
            if result['type'] != 'ok': raise RuntimeError('Fixture SQL failed: '+result.get('error', {}).get('code', 'unknown'))
        return results[-1]['response']['result']['rows']

    def pull(self, revision=b''):
        body = b'\x1a'+varint(len(revision))+revision if revision else b''
        request = urllib.request.Request(self.url+'/pull-updates', data=body, headers={**self.headers, 'Content-Type': 'application/protobuf', 'Accept': 'application/protobuf'})
        with urllib.request.urlopen(request, timeout=30) as response:
            wire = response.read(MAX_RESPONSE+1)
            assert len(wire) <= MAX_RESPONSE
            data = gzip.decompress(wire) if response.headers.get('Content-Encoding') == 'gzip' else wire
        stream = io.BytesIO(data)
        header = read_header(stream)
        if header is None: return revision, {}, {'wire_bytes': len(wire), 'empty': True}
        assert header.get(5, 0) == 0 and 3 in header, 'Expected raw page stream'
        pages = {}
        while (page := read_header(stream)) is not None:
            payload = page.get(2, b'')
            assert len(payload) == 4096
            page_id = page.get(1, 0)
            assert page_id not in pages, 'Duplicate page in response'
            pages[page_id] = payload
        return header[1], pages, {'wire_bytes': len(wire), 'protocol': header.get(8, 0), 'database_pages': header.get(2, 0)}

def image_state(path, pages, size):
    with path.open('wb') as target:
        target.truncate(size*4096)
        for page, data in pages.items():
            target.seek(page*4096)
            target.write(data)
    with sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1', uri=True) as db:
        return db.execute('SELECT count(*), min(marker), max(marker), (SELECT marker FROM page_probe WHERE id=129) FROM page_probe').fetchone()


def cli(*args):
    result = subprocess.run(['turso', *args], capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError('Turso CLI operation failed: ' + ' '.join(args[:2]))
    return result.stdout.strip()


def save(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(path)


@contextmanager
def owned_endpoint(args, directory, report):
    if args.cloud:
        name = 'tursosync-page-probe-' + uuid.uuid4().hex[:12]
        report['cloud_database'] = name
        report['cloud_database_removed'] = False
        save(args.output, report)
        created = False
        try:
            cli('db', 'create', name, '--group', args.group, '--size-limit', '8mb', '--wait')
            created = True
            endpoint = Endpoint(cli('db', 'show', name, '--http-url'),
                                cli('db', 'tokens', 'create', name, '--expiration', '1d'))
            yield endpoint
        finally:
            if created:
                cli('db', 'destroy', name, '--yes')
                report['cloud_database_removed'] = True
            save(args.output, report)
    else:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        with (directory / 'server.log').open('w') as log:
            process = subprocess.Popen([str(args.local_binary.resolve()), '--sync-server',
                                        '127.0.0.1:' + str(port)], cwd=directory,
                                       stdout=log, stderr=subprocess.STDOUT)
            try:
                endpoint = Endpoint('http://127.0.0.1:' + str(port))
                deadline = time.monotonic() + 20
                while True:
                    try:
                        endpoint.sql('SELECT 1')
                        break
                    except (OSError, urllib.error.URLError):
                        if process.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError('Local control server did not start')
                        time.sleep(0.1)
                yield endpoint
            finally:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=15)


def experiment(endpoint, directory, args, report):
    endpoint.sql('CREATE TABLE page_probe(id INTEGER PRIMARY KEY, marker INTEGER NOT NULL, payload BLOB NOT NULL)',
                 'WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<256) '
                 'INSERT INTO page_probe SELECT x,100,randomblob(4096) FROM n')
    revision, baseline, initial = endpoint.pull()
    assert len(baseline) == initial['database_pages'], 'Expected a complete baseline image'
    path = directory / 'image.db'
    assert image_state(path, baseline, initial['database_pages']) == (256, 100, 100, 100)
    report['initial'] = initial
    report['sqlite_version'] = endpoint.sql('SELECT sqlite_version()')[0][0]['value']
    report['journal_mode'] = endpoint.sql('PRAGMA journal_mode')[0][0]['value']
    _, control, _ = endpoint.pull(revision)
    assert not control, 'Unchanged database returned pages'
    endpoint.sql('UPDATE page_probe SET marker=101 WHERE id=129')
    started = time.monotonic()
    commits = 1
    latest_revision = revision
    latest_image = baseline

    def measure(label):
        nonlocal latest_revision, latest_image
        latest_revision, pages, metrics = endpoint.pull(revision)
        changed = [p for p, data in pages.items() if baseline.get(p) != data]
        groups = collections.Counter(p // 32 for p in pages)
        metrics.update(label=label, commits_since_baseline=commits,
                       elapsed_seconds=round(time.monotonic() - started, 3),
                       returned_pages=len(pages), changed_pages=len(changed),
                       unchanged_pages=len(pages) - len(changed),
                       changed_page_ids=sorted(changed), returned_page_ids=sorted(pages),
                       complete_32_page_groups=sum(n == 32 for n in groups.values()))
        expected = 101 if commits % 2 else 100
        latest_image = {**baseline, **pages}
        state = image_state(path, latest_image, metrics.get('database_pages', initial['database_pages']))
        assert state[0] == 256 and state[3] == expected, 'Pull lost a committed update'
        metrics['verified_marker'] = state[3]
        _, recent, _ = endpoint.pull(latest_revision)
        assert not recent, 'Same-revision control returned pages'
        report['steps'].append(metrics)
        save(args.output, report)
        print(label, 'commits', commits, 'returned', len(pages), 'changed', len(changed),
              'unchanged', len(pages) - len(changed), flush=True)

    for seconds in args.ages:
        while time.monotonic() - started < seconds:
            time.sleep(min(1, seconds - (time.monotonic() - started)))
        measure('age_' + str(seconds) + 's')
    for target in args.commits:
        while commits < target:
            count = min(50, target - commits)
            endpoint.sql(*(['UPDATE page_probe SET marker=201-marker WHERE id=129'] * count))
            commits += count
        measure('commit_' + str(target))
    # A recent client must still receive the next real write, even if the old one became coarse.
    endpoint.sql('UPDATE page_probe SET marker=marker+1 WHERE id=129')
    _, pages, metrics = endpoint.pull(latest_revision)
    assert pages, 'Recent client missed a committed write'
    state = image_state(path, {**latest_image, **pages}, metrics.get('database_pages', initial['database_pages']))
    assert state[0] == 256 and state[3] == (101 if commits % 2 else 100) + 1
    changed = sum(latest_image.get(p) != data for p, data in pages.items())
    report['recent_revision_control'] = {**metrics, 'returned_pages': len(pages),
                                         'changed_pages': changed, 'unchanged_pages': len(pages) - changed}
    report['completed'] = True


def sequence(value):
    numbers = sorted(set(int(n) for n in value.split(',') if n))
    if any(n < 0 for n in numbers):
        raise argparse.ArgumentTypeError('Values must be nonnegative')
    return numbers


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--cloud', action='store_true', help='Create and destroy an owned cloud database using Turso CLI')
    mode.add_argument('--local-binary', type=pathlib.Path, help='tursodb binary supporting --sync-server')
    parser.add_argument('--group', default='default')
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--ages', type=sequence, default=[0], help='Seconds to wait with one committed change')
    parser.add_argument('--commits', type=sequence, default=[1, 65, 129, 257, 513, 769, 961, 993, 1001, 1009, 1025, 1101],
                        help='Total commits since the original baseline to measure')
    args = parser.parse_args()
    args.output = args.output.resolve()
    report = {'fixture_rows': 256, 'payload_bytes_per_row': 4096, 'steps': [],
              'age_checkpoints_seconds': args.ages, 'commit_checkpoints': args.commits,
              'mode': 'cloud' if args.cloud else 'local', 'completed': False}
    try:
        with tempfile.TemporaryDirectory(prefix='tursosync-page-probe-') as temporary:
            directory = pathlib.Path(temporary)
            with owned_endpoint(args, directory, report) as endpoint:
                experiment(endpoint, directory, args, report)
    except Exception as ex:
        report['failure_type'] = type(ex).__name__
        raise
    finally:
        save(args.output, report)


if __name__ == '__main__':
    main()

