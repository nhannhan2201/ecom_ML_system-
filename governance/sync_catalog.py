"""
DataHub Metadata & Lineage Synchronization Script (Production Pattern)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System

Nhiệm vụ:
  1. Đọc mô hình Catalog tĩnh từ 'catalog.py' (0% phụ thuộc Spark/MinIO).
  2. Đồng bộ toàn bộ Datasets, Schemas, Tags, Domains lên DataHub GMS.
  3. Xây dựng đồ thị Lineage End-to-End (Raw S3/Kafka -> Bronze -> Silver -> Gold -> Redis).
  4. Đăng ký Data Contracts & Assertions sẵn sàng kiểm soát chất lượng.
"""

import os
import time
import argparse

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    # Dataset Metadata
    DatasetPropertiesClass,
    AuditStampClass,
    GlobalTagsClass,
    TagAssociationClass,
    DomainsClass,
    # Schema Metadata
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    TimeTypeClass,
    BooleanTypeClass,
    OtherSchemaClass,
    # Lineage
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    # Assertions & Data Contracts
    AssertionInfoClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    AssertionStdOperatorClass,
    DataContractPropertiesClass,
    DataContractStatusClass,
    DataContractStateClass,
    DataQualityContractClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)

# Import Declarative Catalog
from catalog import (
    DATASETS,
    DATA_PRODUCTS,
    RAW_BATCH_URN,
    RAW_STAGING_STREAM_URN,
    BRONZE_URN,
    SILVER_URN,
    GOLD_DIM_PROD_URN,
    GOLD_DIM_USER_URN,
    GOLD_FACT_EVENTS_URN,
    GOLD_FEAT_30D_URN,
    GOLD_USER_LABELS_URN,
)


def map_datahub_type(type_name: str) -> SchemaFieldDataTypeClass:
    """Ánh xạ kiểu dữ liệu sang DataHub DataTypeClass chuẩn."""
    tn = type_name.lower()
    if tn == "number":
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    elif tn == "time":
        return SchemaFieldDataTypeClass(type=TimeTypeClass())
    elif tn == "boolean":
        return SchemaFieldDataTypeClass(type=BooleanTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())


def sync_datahub_catalog(gms_url: str = "http://localhost:8089"):
    """Synchronize declarative catalog metadata, schemas, lineage, and contracts to DataHub GMS.

    Iterates over all datasets in catalog.py, generating and emitting MetadataChangeProposal (MCP)
    events to DataHub GMS via REST API. Configures DatasetProperties, SchemaMetadata, UpstreamLineage,
    AssertionInfo, and DataContract aspects, as well as DataFlows and DataJobs representing DP1-DP3.

    Args:
        gms_url: Target DataHub GMS endpoint URL (e.g. 'http://localhost:8089' from host or
                 'http://datahub-gms:8080' from inside Docker network).
    """
    print("=" * 80)
    print("[DATAHUB GOVERNANCE] DONG BO DATA CATALOG & END-TO-END LINEAGE")
    print("=" * 80)
    print(f"DataHub GMS Endpoint : {gms_url}")
    print(f"So luong Datasets    : {len(DATASETS)} bang")
    print(f"So luong DataProducts: {len(DATA_PRODUCTS)} goi san pham")
    print("-" * 80)

    emitter = DatahubRestEmitter(gms_url)
    audit_stamp = AuditStampClass(time=int(time.time() * 1000), actor="urn:li:corpuser:data_engineer")

    t0 = time.time()
    lineage_edges_count = 0
    assertions_count = 0
    contracts_count = 0

    for key, spec in DATASETS.items():
        print(f"  • Đang đồng bộ Dataset: [{spec.platform.upper()}] {spec.name}...")

        # 1. Thuộc tính chung của Dataset (Properties)
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=spec.urn,
                aspect=DatasetPropertiesClass(
                    name=spec.name,
                    description=spec.description,
                    customProperties={"platform": spec.platform, "key": spec.key},
                ),
            )
        )

        # 2. Gắn nhãn (Tags)
        if spec.tags:
            tag_associations = [TagAssociationClass(tag=f"urn:li:tag:{t}") for t in spec.tags]
            emitter.emit(
                MetadataChangeProposalWrapper(entityUrn=spec.urn, aspect=GlobalTagsClass(tags=tag_associations))
            )

        # 3. Gắn phân vùng miền dữ liệu (Domains)
        if spec.domain:
            emitter.emit(MetadataChangeProposalWrapper(entityUrn=spec.urn, aspect=DomainsClass(domains=[spec.domain])))

        # 4. Schema Metadata (Danh sách các cột & kiểu dữ liệu)
        fields = []
        for f in spec.schema_fields:
            fields.append(
                SchemaFieldClass(
                    fieldPath=f.name,
                    type=map_datahub_type(f.type_name),
                    nativeDataType=f.type_name.upper(),
                    description=f.description,
                    isPartOfKey=f.is_primary_key,
                    nullable=f.nullable,
                )
            )

        schema_metadata = SchemaMetadataClass(
            schemaName=spec.name,
            platform=f"urn:li:dataPlatform:{spec.platform}",
            version=0,
            created=audit_stamp,
            lastModified=audit_stamp,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=spec.urn, aspect=schema_metadata))

        # 5. Phả hệ dữ liệu (Upstream Lineage)
        if spec.upstreams:
            upstream_classes = [
                UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED) for u in spec.upstreams
            ]
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=spec.urn, aspect=UpstreamLineageClass(upstreams=upstream_classes)
                )
            )
            lineage_edges_count += len(spec.upstreams)

        # 6. Data Contracts & Assertions
        if spec.contract:
            contract_spec = spec.contract
            quality_assertions = []

            for as_spec in contract_spec.assertions:
                # Đăng ký định nghĩa Assertion
                as_info = AssertionInfoClass(
                    type=AssertionTypeClass.DATASET,
                    description=as_spec.description,
                    datasetAssertion=DatasetAssertionInfoClass(
                        dataset=spec.urn,
                        scope=DatasetAssertionScopeClass.DATASET_ROWS,
                        operator=AssertionStdOperatorClass.EQUAL_TO,
                        logic=as_spec.logic,
                        nativeType="CUSTOM",
                    ),
                )
                emitter.emit(MetadataChangeProposalWrapper(entityUrn=as_spec.urn, aspect=as_info))
                quality_assertions.append(DataQualityContractClass(assertion=as_spec.urn))
                assertions_count += 1

            # Đăng ký và kích hoạt Data Contract
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=contract_spec.urn,
                    aspect=DataContractPropertiesClass(entity=spec.urn, dataQuality=quality_assertions),
                )
            )
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=contract_spec.urn, aspect=DataContractStatusClass(state=DataContractStateClass.ACTIVE)
                )
            )
            contracts_count += 1

    # 7. Đồng bộ Airflow Pipelines & Tasks (Đảm bảo Lineage Task chuẩn xác)
    flow_dp1 = "urn:li:dataFlow:(airflow,dp1_raw_to_bronze,PROD)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=flow_dp1,
            aspect=DataFlowInfoClass(
                name="dp1_raw_to_bronze", description="Pipeline DP1: Ingest Raw Data vào Bronze Delta Lake"
            ),
        )
    )
    job_dp1_ingest = f"urn:li:dataJob:({flow_dp1},ingest_stage)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp1_ingest,
            aspect=DataJobInfoClass(
                name="ingest_stage", type="SPARK", description="Nạp CSV và Flink Staging vào Bronze"
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp1_ingest,
            aspect=DataJobInputOutputClass(
                inputDatasets=[RAW_BATCH_URN, RAW_STAGING_STREAM_URN], outputDatasets=[BRONZE_URN]
            ),
        )
    )

    flow_dp2 = "urn:li:dataFlow:(airflow,dp2_bronze_to_silver_and_gold,PROD)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=flow_dp2,
            aspect=DataFlowInfoClass(
                name="dp2_bronze_to_silver_and_gold",
                description="Pipeline DP2: Khử trùng lặp Bronze vào Silver & Gold DWH",
            ),
        )
    )
    job_dp2_ingest = f"urn:li:dataJob:({flow_dp2},ingest_stage)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp2_ingest,
            aspect=DataJobInfoClass(
                name="ingest_stage",
                type="SPARK",
                description="Khử trùng lặp Bronze nạp vào Silver stg_events và xây dựng Gold DWH (dim_product, dim_user, fact_user_events)",
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp2_ingest,
            aspect=DataJobInputOutputClass(
                inputDatasets=[BRONZE_URN],
                outputDatasets=[SILVER_URN, GOLD_DIM_PROD_URN, GOLD_DIM_USER_URN, GOLD_FACT_EVENTS_URN],
            ),
        )
    )

    job_dp2_validate = f"urn:li:dataJob:({flow_dp2},validate_stage)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp2_validate,
            aspect=DataJobInfoClass(
                name="validate_stage",
                type="SPARK",
                description="Kiểm định chất lượng Silver và quan hệ khóa ngoại Dim - Fact",
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp2_validate,
            aspect=DataJobInputOutputClass(
                inputDatasets=[SILVER_URN, GOLD_DIM_PROD_URN, GOLD_DIM_USER_URN, GOLD_FACT_EVENTS_URN],
                outputDatasets=[],
            ),
        )
    )

    flow_dp3 = "urn:li:dataFlow:(airflow,dp3_compute_offline_features,PROD)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=flow_dp3,
            aspect=DataFlowInfoClass(
                name="dp3_compute_offline_features",
                description="Pipeline DP3: Tính offline features 30d và user labels",
            ),
        )
    )
    job_dp3_ingest = f"urn:li:dataJob:({flow_dp3},ingest_stage)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp3_ingest,
            aspect=DataJobInfoClass(
                name="ingest_stage", type="SPARK", description="Tính đặc trưng 30 ngày cho Feast và gán nhãn nhị phân"
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp3_ingest,
            aspect=DataJobInputOutputClass(
                inputDatasets=[SILVER_URN], outputDatasets=[GOLD_FEAT_30D_URN, GOLD_USER_LABELS_URN]
            ),
        )
    )

    job_dp3_validate = f"urn:li:dataJob:({flow_dp3},validate_stage)"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp3_validate,
            aspect=DataJobInfoClass(
                name="validate_stage",
                type="SPARK",
                description="Kiểm định hợp đồng đặc trưng Feast (event_timestamp, created) và nhãn nhị phân",
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_dp3_validate,
            aspect=DataJobInputOutputClass(inputDatasets=[GOLD_FEAT_30D_URN, GOLD_USER_LABELS_URN], outputDatasets=[]),
        )
    )

    duration = time.time() - t0
    print("-" * 80)
    print(f"[OK] [DONG BO DATAHUB THANH CONG] Thoi gian: {duration:.2f}s")
    print(f"   * Tong so Datasets da tao    : {len(DATASETS)}")
    print(f"   * Tong so Lineage Edges      : {lineage_edges_count}")
    print(f"   * Tong so Assertions dang ky : {assertions_count}")
    print(f"   * Tong so Data Contracts     : {contracts_count}")
    print("=" * 80)
    print("DataHub UI: http://localhost:9002")


def main():
    """Command-line entry point to sync catalog to DataHub GMS."""
    parser = argparse.ArgumentParser(description="Sync Declarative Governance Catalog to DataHub")
    parser.add_argument(
        "--gms-url", default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"), help="DataHub GMS REST URL"
    )
    args = parser.parse_args()

    sync_datahub_catalog(gms_url=args.gms_url)


if __name__ == "__main__":
    main()
