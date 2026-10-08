"""Row-preserving Raw -> Bronze with explicit smoke/full modes.

Runtime requires Spark 3.5.0 and Delta 3.0.0. JVM dependencies are resolved
from configured Maven coordinates; first startup requires network/cache access.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlparse

import yaml

COLUMNS = ['event_time', 'event_type', 'product_id', 'category_id',
           'category_code', 'brand', 'price', 'user_id', 'user_session']


def write_json(path, value):
    """Atomic local report publication; callers own overwrite policy."""
    path = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
            temporary = stream.name
            json.dump(value, stream, indent=2)
            stream.write('\n')
        os.replace(temporary, path)
    finally:
        if temporary and Path(temporary).exists():
            Path(temporary).unlink()


def s3_location(uri):
    parsed = urlparse(uri)
    if parsed.scheme != 's3a' or not parsed.netloc or not parsed.path.strip('/'):
        raise ValueError('Expected an explicit s3a://bucket/key URI')
    if parsed.query or parsed.fragment or any(c in uri for c in '*?[]'):
        raise ValueError('Wildcards, queries and fragments are forbidden')
    return parsed.netloc, parsed.path.lstrip('/')


def load_config(path):
    config = yaml.safe_load(Path(path).read_text())
    for version in ('old', 'new'):
        s3_location(config['input'][version])
    s3_location(config['output_root'])
    s3_location(config['full_output_root'])
    if config['input']['old'] == config['input']['new']:
        raise ValueError('OLD and NEW must be separate objects')
    return config


def create_spark(config):
    from importlib.metadata import version
    from pyspark.sql import SparkSession
    if version('pyspark') != '3.5.0' or version('delta-spark') != '3.0.0':
        raise ValueError('Runtime requires pyspark==3.5.0 and delta-spark==3.0.0')
    access = os.environ.get('MINIO_ACCESS_KEY') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret = os.environ.get('MINIO_SECRET_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not access or not secret:
        raise ValueError('MinIO credentials required in environment')
    settings = config['spark']
    # Redact before configuring sensitive values. Never print the configuration.
    builder = (SparkSession.builder.master(settings['master']).appName('RawToBronze')
               .config('spark.redaction.regex', '(?i)secret|password|token|access[.]?key|credential')
               .config('spark.ui.enabled', 'false')
               .config('spark.jars.packages', ','.join(settings['packages']))
               .config('spark.driver.memory', settings['driver_memory'])
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', settings['shuffle_partitions'])
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .config('spark.hadoop.fs.s3a.endpoint', os.environ.get('MINIO_ENDPOINT') or config['endpoint'])
               .config('spark.hadoop.fs.s3a.path.style.access', 'true')
               .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                       'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
               .config('spark.hadoop.fs.s3a.access.key', access)
               .config('spark.hadoop.fs.s3a.secret.key', secret))
    # Package resolution occurs before SparkSession starts; cached JARs are reused.
    return builder.getOrCreate()


def read_csv(spark, uri, version, limit):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType
    columns = COLUMNS + (['discount_percent'] if version == 'new' else [])
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    frame = (spark.read.schema(schema).options(
        header=True, inferSchema=False, enforceSchema=False, mode='FAILFAST',
        quote='"', escape='"', encoding='UTF-8',
        ignoreLeadingWhiteSpace=False, ignoreTrailingWhiteSpace=False,
        unescapedQuoteHandling='RAISE_ERROR').csv(uri))
    if limit is not None:
        frame = frame.limit(limit)
    # CSV represents source fields as text; preserve empty field values as ''.
    frame = frame.select(*[F.coalesce(F.col(c), F.lit('')).alias(c) for c in columns])
    if version == 'old':
        frame = frame.withColumn('discount_percent', F.lit(None).cast('string'))
    return frame.withColumn('_source_object', F.lit(uri)).withColumn('_schema_version', F.lit(version))


def build_bronze(old, new, run_id, timestamp):
    from pyspark.sql import functions as F
    return (old.unionByName(new).withColumn('_run_id', F.lit(run_id))
            .withColumn('_ingested_at', F.lit(timestamp).cast('timestamp')))


def write_bronze(frame, destination):
    frame.write.format('delta').mode('errorifexists').save(destination)


def verify_bronze(expected, actual, checks=None, expected_counts=None):
    checks = checks if checks is not None else {}
    if expected.schema != actual.schema:
        # Delta may widen field nullability; names/types/order must remain exact.
        if [(f.name, f.dataType) for f in expected.schema] != [(f.name, f.dataType) for f in actual.schema]:
            raise ValueError('Readback schema mismatch')
    checks['schema_names_types_order'] = 'PASS'
    before = {r['_schema_version']: r['count'] for r in expected.groupBy('_schema_version').count().collect()}
    after = {r['_schema_version']: r['count'] for r in actual.groupBy('_schema_version').count().collect()}
    if before != after:
        raise ValueError('Readback counts mismatch')
    checks['readback_counts_per_schema'] = {'status': 'PASS', 'input': before, 'output': after}
    if expected_counts is not None:
        if before != expected_counts or after != expected_counts:
            raise ValueError('Full counts differ from source manifest output_counts')
        checks['full_manifest_counts'] = {'status': 'PASS', 'expected': expected_counts}
    else:
        checks['full_manifest_counts'] = {'status': 'NOT_APPLICABLE', 'scope': 'smoke'}
    if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
        raise ValueError('Readback values/multiplicities mismatch')
    checks['values_and_multiplicities_bidirectional_exceptAll'] = 'PASS'
    from pyspark.sql import functions as F
    if actual.filter((F.col('_schema_version') == 'old') & F.col('discount_percent').isNotNull()).limit(1).count():
        raise ValueError('OLD discount must be null')
    checks['old_discount_null'] = 'PASS'
    # NEW values, including discount and provenance, are covered by full-row exceptAll.
    return before


def object_binding(client, inputs):
    result = {}
    for version in ('old', 'new'):
        bucket, key = s3_location(inputs[version])
        head = client.head_object(Bucket=bucket, Key=key)
        result[version] = {'bytes': head['ContentLength'], 'etag': head['ETag'],
                           'modified': head['LastModified'].isoformat()}
    return result


def ensure_output_bucket(client, bucket):
    """Provision only a confirmed-missing destination bucket on local MinIO."""
    from botocore.exceptions import ClientError
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        code = str(error.response.get('Error', {}).get('Code', ''))
        status = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if code not in ('404', 'NoSuchBucket', 'NotFound') or status != 404:
            raise
        client.create_bucket(Bucket=bucket)
        client.head_bucket(Bucket=bucket)


def preflight(client, config, destination):
    import csv
    evidence = json.loads(Path(config['input']['evidence']).read_text())
    if evidence['status'] != 'VERIFIED_DECLARED_CHECKS' or evidence['readback']['status'] != 'READBACK_PASS':
        raise ValueError('Source evidence is not PASS')
    manifest = evidence['manifest']
    bucket, prefix = manifest['bucket'], manifest['prefix']
    bindings = object_binding(client, config['input'])
    for v in ('old', 'new'):
        name = f'raw_events_{v}.csv'
        if s3_location(config['input'][v]) != (bucket, prefix + '/' + name):
            raise ValueError('Input URI differs from verified source evidence')
        if bindings[v]['bytes'] != manifest['objects'][name]['bytes']:
            raise ValueError('Input size differs from evidence')
        response = client.get_object(Bucket=bucket, Key=prefix + '/' + name, Range='bytes=0-4095')
        try:
            first = response['Body'].read().decode('utf-8').splitlines()[0]
        finally:
            response['Body'].close()
        if next(csv.reader([first])) != COLUMNS + (['discount_percent'] if v == 'new' else []):
            raise ValueError('Unexpected CSV header')
    response = client.get_object(Bucket=bucket, Key=prefix + '/manifest.json')
    try:
        payload = response['Body'].read()
    finally:
        response['Body'].close()
    if hashlib.sha256(payload).hexdigest() != evidence['artifact_hashes']['manifest.json']['sha256']:
        raise ValueError('Remote manifest hash differs from source evidence')
    out_bucket, out_prefix = s3_location(destination)
    if out_bucket == bucket and (out_prefix == prefix or out_prefix.startswith(prefix + '/')):
        raise ValueError('Output overlaps raw prefix')
    ensure_output_bucket(client, out_bucket)
    if client.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix + '/', MaxKeys=1).get('KeyCount', 0):
        raise ValueError('Output prefix is not empty')
    return bindings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config/spark_config.yaml')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--mode', choices=['smoke', 'full'], default='smoke')
    parser.add_argument('--limit-per-schema', type=int)
    parser.add_argument('--evidence-output', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.run_id):
        parser.error('run-id must contain only letters, digits, - and _')
    config = load_config(args.config)
    if args.mode == 'full' and args.limit_per_schema is not None:
        parser.error('full mode forbids --limit-per-schema')
    limit = None if args.mode == 'full' else (
        args.limit_per_schema if args.limit_per_schema is not None else config['limit_per_schema'])
    if limit is not None and not 1 <= limit <= 1000:
        parser.error('smoke limit must be between 1 and 1000')
    config['spark'] = dict(config['spark'])
    if args.mode == 'full':
        config['spark']['shuffle_partitions'] = config['spark']['full_shuffle_partitions']
    evidence_path = Path(args.evidence_output)
    if evidence_path.exists() or not evidence_path.parent.is_dir():
        parser.error('evidence must be a new file in an existing directory')
    directory = Path('artifacts/spark-raw-to-bronze') / args.run_id
    directory.mkdir(parents=True, exist_ok=False)
    output_root = config['full_output_root'] if args.mode == 'full' else config['output_root']
    destination = output_root.rstrip('/') + '/' + args.run_id
    report = {'status': 'STARTED', 'run_id': args.run_id, 'input': config['input'],
              'output': destination, 'mode': args.mode, 'limit_per_schema': limit, 'checks': {},
              'started_at': datetime.now(timezone.utc).isoformat(),
              'limitations': [('Bounded run; not full October reconciliation.' if args.mode == 'smoke' else
                               'Full October Raw -> Bronze only; no Silver, natural duplicate or ML completeness claim.'),
                              'No independent full input SHA audit or source business identity audit.',
                              'Single writer; input objects must remain unchanged; cache is not a snapshot.',
                              'CSV FAILFAST is not a complete structural audit.',
                              'Field nullability may widen during Delta persistence.']}
    started = time.monotonic()
    spark = frame = None
    try:
        import boto3
        from dotenv import load_dotenv
        from importlib.metadata import version
        load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)
        if not (os.environ.get('MINIO_ACCESS_KEY') or os.environ.get('AWS_ACCESS_KEY_ID')) or not (
                os.environ.get('MINIO_SECRET_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')):
            raise ValueError('MinIO credentials required in environment')
        client = boto3.client('s3', endpoint_url=os.environ.get('MINIO_ENDPOINT') or config['endpoint'],
                              aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY') or os.environ.get('AWS_ACCESS_KEY_ID'),
                              aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY'),
                              region_name='us-east-1')
        before = preflight(client, config, destination)
        report['checks']['source_preflight'] = 'PASS'
        source_evidence_bytes = Path(config['input']['evidence']).read_bytes()
        source_evidence = json.loads(source_evidence_bytes)
        expected_counts = source_evidence['manifest']['output_counts'] if args.mode == 'full' else None
        report.update(status='PREFLIGHT_PASS', input_binding=before,
                      source_evidence_sha256=hashlib.sha256(source_evidence_bytes).hexdigest(),
                      spark_settings=config['spark'])
        print('PREFLIGHT_PASS', flush=True)
        write_json(directory / 'report.json', report)
        spark = create_spark(config)
        old = read_csv(spark, config['input']['old'], 'old', limit)
        new = read_csv(spark, config['input']['new'], 'new', limit)
        from pyspark import StorageLevel
        frame = build_bronze(old, new, args.run_id, report['started_at']).persist(
            StorageLevel.DISK_ONLY if args.mode == 'full' else StorageLevel.MEMORY_AND_DISK)
        selected_counts = {r['_schema_version']: r['count'] for r in
                           frame.groupBy('_schema_version').count().collect()}
        if expected_counts is not None and selected_counts != expected_counts:
            raise ValueError('Full input counts differ from manifest; refusing Bronze write')
        report['selected_input_counts'] = selected_counts
        report['checks']['selected_input_counts'] = 'PASS'
        write_json(directory / 'report.json', report)
        print('INPUT_MATERIALIZED', selected_counts, flush=True)
        write_bronze(frame, destination)
        report['status'] = 'WRITE_SUCCEEDED'
        print('WRITE_SUCCEEDED; starting exact readback (may require disk shuffle)', flush=True)
        write_json(directory / 'report.json', report)
        actual = spark.read.format('delta').load(destination)
        counts = verify_bronze(frame, actual, report['checks'], expected_counts)
        if before != object_binding(client, config['input']):
            raise ValueError('Input object metadata changed during runtime')
        report['checks']['source_metadata_unchanged'] = 'PASS'
        report['checks']['delta_readback'] = 'PASS'
        from delta.tables import DeltaTable
        report.update(status='READBACK_PASS', counts=counts, schema=actual.schema.jsonValue(),
                      delta_version=DeltaTable.forPath(spark, destination).history(1).select('version').first()[0],
                      versions={n: version(n) for n in ('pyspark', 'delta-spark', 'boto3')},
                      code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      finished_at=datetime.now(timezone.utc).isoformat(), elapsed_seconds=time.monotonic() - started)
        write_json(directory / 'report.json', report)
        # Exclusive publish prevents accidentally replacing another report.
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir=evidence_path.parent, delete=False) as stream:
                temporary = stream.name
                json.dump(report, stream, indent=2)
                stream.write('\n')
            os.link(temporary, evidence_path)
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)
        print('READBACK_PASS', destination)
    except Exception as error:
        # Exception strings from connectors can contain credentials. Do not export them.
        from botocore.exceptions import ClientError
        if isinstance(error, ClientError):
            # Whitelist AWS codes/operations; never copy arbitrary exception text.
            code = str(error.response.get('Error', {}).get('Code', ''))
            operation = error.operation_name
            if re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', code) and re.fullmatch(r'[A-Za-z0-9]{1,80}', operation):
                report['s3_error'] = {'code': code, 'operation': operation}
        report.update(status='FAILED', failed_after=report['status'], error_type=type(error).__name__,
                      elapsed_seconds=time.monotonic() - started)
        write_json(directory / 'report.json', report)
        raise RuntimeError(f'Raw -> Bronze failed; see {directory}/report.json (exception text suppressed)') from None
    finally:
        if frame is not None:
            frame.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
