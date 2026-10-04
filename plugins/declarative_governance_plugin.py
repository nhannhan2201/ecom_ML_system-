"""
================================================================================
AIRFLOW PLUGIN: DECLARATIVE GOVERNANCE (OPTION B: METADATA-AS-CODE)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
================================================================================
KIẾN TRÚC GOVERNANCE CỦA DỰ ÁN:
Dự án sử dụng cơ chế Declarative Governance (Metadata-as-Code):
  - Toàn bộ 12 Datasets, Field Schemas, DataFlows, DataJobs (DP1, DP2, DP3) và Data Contracts
    được khai báo tường minh trong `governance/catalog.py`.
  - Việc đồng bộ metadata và lineage lên DataHub GMS được thực thi tập trung qua
    `governance/sync_catalog.py` (sử dụng DataHub REST Emitter).
  - Runtime listener tự động từ Airflow (acryl-datahub-airflow-plugin) không kích hoạt
    để đảm bảo tính tất định, độc lập giữa các container và tránh xung đột phiên bản
    thư viện với Airflow 2.7.3.
================================================================================
"""

import logging
from airflow.plugins_manager import AirflowPlugin

logger = logging.getLogger("airflow.plugins.declarative_governance")


class DeclarativeGovernancePlugin(AirflowPlugin):
    """
    Airflow Declarative Governance Plugin.
    Lineage và DataJobs được quản lý tập trung và phát sinh tường minh qua governance/sync_catalog.py.
    """
    name = "declarative_governance_plugin"
    listeners = []


logger.info("ℹ️ [DeclarativeGovernancePlugin]: Chế độ Declarative Governance đang hoạt động (sync qua governance/sync_catalog.py).")
