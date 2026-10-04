"""
QUICKSTART: MINIO OBJECT STORAGE (STORAGE LAYER - L8)
Script này dùng thư viện chuẩn AWS S3 SDK (boto3) để tương tác với MinIO:
1. Kết nối vào MinIO S3 API qua cổng 9000
2. Tạo 2 bucket chuẩn cho dự án: 'ecommerce-raw' và 'ecommerce-lakehouse'
3. Upload 1 file thử nghiệm để kiểm chứng tính năng ghi dữ liệu
"""

import boto3
from botocore.client import Config

def main():
    # 1. KHỞI TẠO S3 CLIENT KẾT NỐI TỚI MINIO
    # - endpoint_url: Cổng 9000 là cổng tiếp nhận lệnh S3 API của MinIO
    # - aws_access_key_id / secret_access_key: Tài khoản root đã khai báo trong docker-compose
    # - signature_version='s3v4': Chuẩn bảo mật ký tên phiên bản 4 của AWS S3
    print("⏳ Đang kết nối tới MinIO tại http://localhost:9000...")
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # 2. DANH SÁCH BUCKET CẦN KHỞI TẠO CHO TOÀN BỘ HỆ THỐNG
    # - ecommerce-raw: Chứa dữ liệu CSV thô ban đầu (từ file 2019-Oct.csv hoặc generator)
    # - ecommerce-lakehouse: Chứa các tầng Medallion Delta Lake (bronze, silver, gold)
    target_buckets = ['ecommerce-raw', 'ecommerce-lakehouse']

    print("\n--- BƯỚC 1: KIỂM TRA VÀ TẠO BUCKET ---")
    # Lấy danh sách các bucket hiện đang có trên MinIO
    response = s3_client.list_buckets()
    existing_buckets = [b['Name'] for b in response.get('Buckets', [])]

    for bucket_name in target_buckets:
        if bucket_name not in existing_buckets:
            # Gọi API tạo bucket
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Đã tạo mới bucket: '{bucket_name}'")
        else:
            print(f"ℹ️ Bucket '{bucket_name}' đã tồn tại sẵn.")

    # 3. GHI THỬ MỘT FILE TEST LÊN MINIO ĐỂ KIỂM CHỨNG (PUT OBJECT)
    print("\n--- BƯỚC 2: UPLOAD FILE TEST LÊN BUCKET 'ecommerce-raw' ---")
    test_file_key = "test_folder/hello_minio.txt"
    test_content = (
        "Xin chao MinIO!\n"
        "Day la du lieu kiem thu ket noi dau tien cua he thong E-commerce ML.\n"
        "Storage Layer L8 hoat dong hoan hao!"
    )

    s3_client.put_object(
        Bucket='ecommerce-raw',
        Key=test_file_key,
        Body=test_content.encode('utf-8')
    )
    print(f"✅ Đã upload thành công object: '{test_file_key}' vào bucket 'ecommerce-raw'")

    # 4. ĐỌC LẠI NỘI DUNG TỪ MINIO ĐỂ XÁC NHẬN (GET OBJECT)
    print("\n--- BƯỚC 3: ĐỌC LẠI DỮ LIỆU TỪ MINIO ĐỂ XÁC MINH ---")
    obj = s3_client.get_object(Bucket='ecommerce-raw', Key=test_file_key)
    downloaded_text = obj['Body'].read().decode('utf-8')
    print("📄 Nội dung file vừa đọc lại từ MinIO:")
    print("-" * 40)
    print(downloaded_text)
    print("-" * 40)

    print("\n🎉 THÀNH CÔNG! Bây giờ bạn hãy mở trình duyệt Web (http://localhost:9001) và F5 để xem kết quả!")

if __name__ == '__main__':
    main()
