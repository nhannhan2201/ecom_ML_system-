"""Pure checks run without Spark; native fixture checks require pinned runtime/JARs."""
import csv
import io
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from src.spark.raw_to_bronze import (
    COLUMNS, build_bronze, ensure_output_bucket, load_config, preflight, read_csv, s3_location,
    verify_bronze, write_bronze, write_json,
)


@pytest.mark.parametrize('uri', ['s3://bucket/key', 's3a://bucket/', 's3a://bucket/*.csv',
                                 's3a://bucket/key?x=1', 'file:///tmp/input'])
def test_reject_ambiguous_uri(uri):
    with pytest.raises(ValueError):
        s3_location(uri)


def test_config_binds_explicit_objects():
    config = load_config('config/spark_config.yaml')
    assert config['input']['old'].endswith('/raw_events_old.csv')
    assert config['input']['new'].endswith('/raw_events_new.csv')
    assert config['limit_per_schema'] == 1000


def test_atomic_report(tmp_path):
    target = tmp_path / 'report.json'
    write_json(target, {'status': 'FAILED'})
    assert target.read_text().strip() == '{\n  "status": "FAILED"\n}'
    assert list(tmp_path.iterdir()) == [target]


def test_preflight_rejects_wrong_input_before_write():
    config = load_config('config/spark_config.yaml')
    config['input']['old'] = 's3a://other/unverified.csv'
    client = Mock()
    client.head_object.return_value = {'ContentLength': 1, 'ETag': 'etag',
                                       'LastModified': datetime.now(timezone.utc)}
    with pytest.raises(ValueError, match='differs from verified'):
        preflight(client, config, config['output_root'] + '/test')
    client.put_object.assert_not_called()


@pytest.fixture(scope='module')
def spark():
    from importlib.metadata import PackageNotFoundError, version
    try:
        versions = (version('pyspark'), version('delta-spark'))
    except PackageNotFoundError:
        pytest.skip('Native Spark fixtures need pyspark 3.5.0 / delta-spark 3.0.0 and Delta JARs')
    if versions != ('3.5.0', '3.0.0'):
        pytest.skip('Native fixtures require pinned Spark/Delta versions')
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master('local[2]').appName('BronzeFixture')
               .config('spark.ui.enabled', 'false')
               .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.0.0')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', '2')
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .getOrCreate())
    yield session
    session.stop()


def fixture_csv(path, version, rows):
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(COLUMNS + (['discount_percent'] if version == 'new' else []))
        writer.writerows(rows)
    return str(path)


def test_native_csv_union_delta_roundtrip(spark, tmp_path):
    row = ['2019-10-15 23:59:59 UTC', 'view', '000123', '900000000000000001',
           '', ' spaced, "brand" ', '01.20', '42', 'null']
    old = read_csv(spark, fixture_csv(tmp_path / 'old.csv', 'old', [row, row]), 'old', 1000)
    new = read_csv(spark, fixture_csv(tmp_path / 'new.csv', 'new',
                                     [['2019-10-16 00:00:00 UTC'] + row[1:] + ['4']]), 'new', 1000)
    expected = build_bronze(old, new, 'fixture', '2026-10-08T00:00:00+00:00').cache()
    assert expected.count() == 3
    original = expected.filter("_schema_version = 'old'").first()
    assert original.product_id == '000123'
    assert original.brand == row[5]
    assert original.price == '01.20'
    assert original.category_code == ''
    assert original.user_session == 'null'
    assert original.discount_percent is None
    destination = str(tmp_path / 'bronze')
    write_bronze(expected, destination)
    actual = spark.read.format('delta').load(destination)
    checks = {}
    assert verify_bronze(expected, actual, checks, {'old': 2, 'new': 1}) == {'old': 2, 'new': 1}
    assert checks['full_manifest_counts']['status'] == 'PASS'
    assert checks['values_and_multiplicities_bidirectional_exceptAll'] == 'PASS'
    with pytest.raises(ValueError, match='Full counts differ'):
        verify_bronze(expected, actual, {}, {'old': 3, 'new': 1})
    from pyspark.sql import functions as F
    with pytest.raises(ValueError, match='values/multiplicities'):
        verify_bronze(expected, actual.withColumn('price', F.lit('changed')))
    with pytest.raises(Exception):
        write_bronze(expected, destination)
    with pytest.raises(ValueError, match='counts mismatch'):
        verify_bronze(expected, actual.dropDuplicates())
    expected.unpersist()


def test_native_wrong_header_fails(spark, tmp_path):
    path = tmp_path / 'bad.csv'
    path.write_text('wrong,' + ','.join(COLUMNS[1:]) + '\n' + ','.join(['x'] * 9) + '\n')
    with pytest.raises(Exception):
        read_csv(spark, str(path), 'old', 1000).collect()


def test_preflight_detects_populated_destination(tmp_path):
    import json
    from pathlib import Path
    evidence = json.loads(Path('docs/evidence/batch_generator_october.json').read_text())
    config = load_config('config/spark_config.yaml')
    client = Mock()
    client.head_object.side_effect = [
        {'ContentLength': evidence['manifest']['objects'][f'raw_events_{v}.csv']['bytes'],
         'ETag': v, 'LastModified': datetime.now(timezone.utc)} for v in ('old', 'new')]
    import hashlib
    payload = json.dumps(evidence['manifest']).encode()
    evidence['artifact_hashes']['manifest.json']['sha256'] = hashlib.sha256(payload).hexdigest()
    evidence_file = tmp_path / 'source_evidence.json'
    evidence_file.write_text(json.dumps(evidence))
    config['input']['evidence'] = str(evidence_file)
    client.get_object.side_effect = [
        {'Body': io.BytesIO((','.join(COLUMNS) + '\n').encode())},
        {'Body': io.BytesIO((','.join(COLUMNS + ['discount_percent']) + '\n').encode())},
        {'Body': io.BytesIO(payload)},
    ]
    client.list_objects_v2.return_value = {'KeyCount': 1}
    with pytest.raises(ValueError, match='not empty'):
        preflight(client, config, config['output_root'] + '/existing')


def test_failure_keeps_evidence_absent(tmp_path, monkeypatch):
    import json
    import sys
    from src.spark import raw_to_bronze as job
    config = load_config('config/spark_config.yaml')
    monkeypatch.setattr(job, 'load_config', lambda _: config)
    def fail(*args):
        raise ValueError('secret-example-must-not-appear')
    monkeypatch.setattr(job, 'preflight', fail)
    monkeypatch.chdir(tmp_path)
    evidence = tmp_path / 'evidence.json'
    monkeypatch.setattr(sys, 'argv', ['job', '--run-id', 'failure', '--evidence-output', str(evidence)])
    with pytest.raises(RuntimeError):
        job.main()
    assert not evidence.exists()
    report = tmp_path / 'artifacts/spark-raw-to-bronze/failure/report.json'
    assert json.loads(report.read_text())['status'] == 'FAILED'
    assert 'secret-example' not in report.read_text()


def test_existing_bucket_is_not_created():
    client = Mock()
    ensure_output_bucket(client, 'destination')
    client.create_bucket.assert_not_called()


@pytest.mark.parametrize('code', ['404', 'NoSuchBucket', 'NotFound'])
def test_missing_destination_created_and_rechecked(code):
    from botocore.exceptions import ClientError
    client = Mock()
    client.head_bucket.side_effect = [ClientError(
        {'Error': {'Code': code}, 'ResponseMetadata': {'HTTPStatusCode': 404}}, 'HeadBucket'), {}]
    ensure_output_bucket(client, 'destination')
    client.create_bucket.assert_called_once_with(Bucket='destination')
    assert client.head_bucket.call_count == 2


@pytest.mark.parametrize('code,status', [('403', 403), ('AccessDenied', 403),
                                         ('InternalError', 500), ('NoSuchBucket', 403)])
def test_other_s3_errors_do_not_create_bucket(code, status):
    from botocore.exceptions import ClientError
    client = Mock()
    client.head_bucket.side_effect = ClientError(
        {'Error': {'Code': code}, 'ResponseMetadata': {'HTTPStatusCode': status}}, 'HeadBucket')
    with pytest.raises(ClientError):
        ensure_output_bucket(client, 'destination')
    client.create_bucket.assert_not_called()


def test_connection_failure_does_not_create_bucket():
    from botocore.exceptions import EndpointConnectionError
    client = Mock()
    client.head_bucket.side_effect = EndpointConnectionError(endpoint_url='http://localhost:9000')
    with pytest.raises(EndpointConnectionError):
        ensure_output_bucket(client, 'destination')
    client.create_bucket.assert_not_called()


def test_create_failure_is_not_ignored():
    from botocore.exceptions import ClientError
    client = Mock()
    client.head_bucket.side_effect = ClientError(
        {'Error': {'Code': '404'}, 'ResponseMetadata': {'HTTPStatusCode': 404}}, 'HeadBucket')
    client.create_bucket.side_effect = ClientError({'Error': {'Code': 'AccessDenied'}}, 'CreateBucket')
    with pytest.raises(ClientError):
        ensure_output_bucket(client, 'destination')
    assert client.head_bucket.call_count == 1


def test_native_full_reader_has_no_limit(spark, tmp_path):
    row = ['2019-10-15 23:59:59 UTC', 'view', '1', '2', '', '', '1.00', '3', 'session']
    uri = fixture_csv(tmp_path / 'full.csv', 'old', [row] * 1005)
    assert read_csv(spark, uri, 'old', None).count() == 1005
    assert read_csv(spark, uri, 'old', 1000).count() == 1000


def test_full_rejects_limit_before_creating_artifacts(tmp_path, monkeypatch):
    import sys
    from src.spark import raw_to_bronze as job
    config = load_config('config/spark_config.yaml')
    monkeypatch.setattr(job, 'load_config', lambda _: config)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, 'argv', ['job', '--mode', 'full', '--run-id', 'full',
                                    '--limit-per-schema', '1000', '--evidence-output', 'new.json'])
    with pytest.raises(SystemExit):
        job.main()
    assert not (tmp_path / 'artifacts').exists()


def test_existing_evidence_is_never_replaced(tmp_path, monkeypatch):
    import sys
    from src.spark import raw_to_bronze as job
    config = load_config('config/spark_config.yaml')
    monkeypatch.setattr(job, 'load_config', lambda _: config)
    monkeypatch.chdir(tmp_path)
    evidence = tmp_path / 'old.json'
    evidence.write_text('historical evidence')
    monkeypatch.setattr(sys, 'argv', ['job', '--mode', 'full', '--run-id', 'full',
                                    '--evidence-output', str(evidence)])
    with pytest.raises(SystemExit):
        job.main()
    assert evidence.read_text() == 'historical evidence'
    assert not (tmp_path / 'artifacts').exists()
