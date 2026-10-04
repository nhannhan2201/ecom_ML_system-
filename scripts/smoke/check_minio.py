"""
QUICKSTART: MINIO OBJECT STORAGE (STORAGE LAYER - L8)
Script này dùng thư viện chuẩn AWS S3 SDK (boto3) để tương tác với MinIO:
1. Kết nối vào MinIO S3 API qua cổng 9000
2. Tạo 2 bucket chuẩn cho dự án: 'ecommerce-raw' và 'ecommerce-lakehouse'
3. Upload 1 file thử nghiệm để kiểm chứng tính năng ghi dữ liệu
"""

import os
import boto3
from botocore.client import Config


def main():
    # 1. KHỞI TẠO S3 CLIENT KẾT NỐI TỚI MINIO
    # - endpoint_url: Cổng 9000 là cổng tiếp nhận lệnh S3 API của MinIO
    # - aws_access_key_id / secret_access_key: Tài khoản root đã khai báo trong docker-compose
    # - signature_version='s3v4': Chuẩn bảo mật ký tên phiên bản 4 của AWS S3
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key:
        raise ValueError("Missing required environment variable: 'MINIO_ACCESS_KEY' (or 'AWS_ACCESS_KEY_ID')")
    if not secret_key:
        raise ValueError("Missing required environment variable: 'MINIO_SECRET_KEY' (or 'AWS_SECRET_ACCESS_KEY')")

    print(f"⏳ Đang kết nối tới MinIO tại {endpoint}...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # 2. DANH SÁCH BUCKET CẦN KHỞI TẠO CHO TOÀN BỘ HỆ THỐNG
    # - ecommerce-raw: Chứa dữ liệu CSV thô ban đầu (từ file 2019-Oct.csv hoặc generator)
    # - ecommerce-lakehouse: Chứa các tầng Medallion Delta Lake (bronze, silver, gold)
    target_buckets = ["ecommerce-raw", "ecommerce-lakehouse"]

    print("\n--- BƯỚC 1: KIỂM TRA VÀ TẠO BUCKET ---")
    # Lấy danh sách các bucket hiện đang có trên MinIO
    response = s3_client.list_buckets()
    existing_buckets = [b["Name"] for b in response.get("Buckets", [])]

    for bucket_name in target_buckets:
        if bucket_name not in existing_buckets:
            # Gọi API tạo bucket
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"[OK] Created bucket: '{bucket_name}'")
        else:
            print(f"[INFO] Bucket '{bucket_name}' already exists.")

    # 3. GHI THỬ MỘT FILE TEST LÊN MINIO ĐỂ KIỂM CHỨNG (PUT OBJECT)
    print("\n--- BƯỚC 2: UPLOAD FILE TEST LÊN BUCKET 'ecommerce-raw' ---")
    test_file_key = "test_folder/hello_minio.txt"
    test_content = (
        "Xin chao MinIO!\n"
        "Day la du lieu kiem thu ket noi dau tien cua he thong E-commerce ML.\n"
        "Storage Layer L8 hoat dong hoan hao!"
    )

    s3_client.put_object(Bucket="ecommerce-raw", Key=test_file_key, Body=test_content.encode("utf-8"))
    print(f"[OK] Uploaded object: '{test_file_key}' into bucket 'ecommerce-raw'")

    # 4. ĐỌC LẠI NỘI DUNG TỪ MINIO ĐỂ XÁC NHẬN (GET OBJECT)
    print("\n--- BƯỚC 3: ĐỌC LẠI DỮ LIỆU TỪ MINIO ĐỂ XÁC MINH ---")
    obj = s3_client.get_object(Bucket="ecommerce-raw", Key=test_file_key)
    downloaded_text = obj["Body"].read().decode("utf-8")
    print("Content read back from MinIO:")
    print("-" * 40)
    print(downloaded_text)
    print("-" * 40)

    print("\n[OK] MinIO Smoke test completed successfully (Console: http://localhost:9001)")


if __name__ == "__main__":
    main()
