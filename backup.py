#!/usr/bin/env python3
import os
import datetime
import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from minio import Minio
from minio.error import S3Error
import io
import json

class OutlineBackupTool:
    def __init__(self, backup_dir="./backups"):
        """初始化备份工具"""
        self.backup_dir = backup_dir
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_backup_dir = os.path.join(self.backup_dir, f"backup_{self.timestamp}")
        
        # PostgreSQL 连接信息
        self.pg_host = "localhost"  # 或者Docker容器的IP
        self.pg_port = 5432
        self.pg_user = "postgres"
        self.pg_password = "your_postgres_password"  # 替换为实际密码
        
        # MinIO 连接信息
        self.minio_endpoint = "minio.codescope.site"
        self.minio_access_key = "admin"
        self.minio_secret_key = "Gcb990217!"
        self.minio_secure = False  # 如果使用HTTPS则设为True
        
        # 确保备份目录存在
        os.makedirs(self.current_backup_dir, exist_ok=True)
        os.makedirs(os.path.join(self.current_backup_dir, "postgres"), exist_ok=True)
        os.makedirs(os.path.join(self.current_backup_dir, "minio"), exist_ok=True)
        
        print(f"备份将保存到: {self.current_backup_dir}")
    
    def backup_postgres(self):
        """使用psycopg2备份PostgreSQL数据库"""
        print("\n=== 开始备份PostgreSQL数据库 ===")
        
        try:
            # 连接到PostgreSQL服务器
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 获取数据库列表
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            all_databases = [row[0] for row in cursor.fetchall()]
            print(f"找到以下数据库: {', '.join(all_databases)}")
            
            # 备份特定数据库
            databases_to_backup = ["outline", "outline_test", "keycloak"]
            for db_name in databases_to_backup:
                if db_name in all_databases:
                    print(f"备份数据库: {db_name}")
                    backup_file = os.path.join(self.current_backup_dir, "postgres", f"{db_name}_backup.sql")
                    
                    # 使用pg_dump命令
                    with open(backup_file, 'w') as f:
                        # 创建pg_dump进程
                        pg_dump_cmd = [
                            "pg_dump", 
                            "-h", self.pg_host,
                            "-p", str(self.pg_port),
                            "-U", self.pg_user,
                            "-d", db_name
                        ]
                        
                        # 设置环境变量PGPASSWORD
                        env = os.environ.copy()
                        env["PGPASSWORD"] = self.pg_password
                        
                        # 执行pg_dump
                        import subprocess
                        process = subprocess.Popen(
                            pg_dump_cmd, 
                            stdout=f, 
                            stderr=subprocess.PIPE,
                            env=env,
                            universal_newlines=True
                        )
                        _, stderr = process.communicate()
                        
                        if process.returncode != 0:
                            print(f"备份 {db_name} 失败: {stderr}")
                        else:
                            print(f"成功备份数据库 {db_name} 到 {backup_file}")
                else:
                    print(f"警告: 数据库 {db_name} 不存在，跳过备份")
            
            cursor.close()
            conn.close()
            print("PostgreSQL数据库备份完成")
            
        except Exception as e:
            print(f"PostgreSQL备份过程中出错: {e}")
    
    def backup_minio(self):
        """使用MinIO Python客户端备份MinIO存储桶"""
        print("\n=== 开始备份MinIO存储桶 ===")
        
        try:
            # 创建MinIO客户端
            minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            
            # 获取所有存储桶
            buckets = minio_client.list_buckets()
            if not buckets:
                print("未找到MinIO存储桶，跳过MinIO备份")
                return
            
            print(f"找到以下存储桶: {', '.join([bucket.name for bucket in buckets])}")
            
            # 备份每个存储桶
            for bucket in buckets:
                bucket_name = bucket.name
                print(f"备份存储桶: {bucket_name}")
                bucket_backup_dir = os.path.join(self.current_backup_dir, "minio", bucket_name)
                os.makedirs(bucket_backup_dir, exist_ok=True)
                
                # 获取桶中的所有对象
                objects = minio_client.list_objects(bucket_name, recursive=True)
                for obj in objects:
                    # 创建目标路径
                    object_name = obj.object_name
                    target_path = os.path.join(bucket_backup_dir, object_name)
                    
                    # 确保目标目录存在
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    
                    # 下载对象
                    try:
                        response = minio_client.get_object(bucket_name, object_name)
                        with open(target_path, 'wb') as file_data:
                            for d in response.stream(32*1024):
                                file_data.write(d)
                        print(f"已备份: {object_name}")
                    except Exception as e:
                        print(f"备份对象 {object_name} 失败: {e}")
                    finally:
                        response.close()
                        response.release_conn()
                
                # 尝试获取并保存桶策略
                try:
                    policy = minio_client.get_bucket_policy(bucket_name)
                    policy_file = os.path.join(bucket_backup_dir, "policy.json")
                    with open(policy_file, 'w') as f:
                        f.write(policy)
                    print(f"已导出 {bucket_name} 的策略到 {policy_file}")
                except Exception as e:
                    print(f"获取 {bucket_name} 的策略失败: {e}")
            
            print("MinIO存储桶备份完成")
            
        except Exception as e:
            print(f"MinIO备份过程中出错: {e}")
    
    def restore_postgres(self, backup_path=None):
        """恢复PostgreSQL数据库"""
        print("\n=== 开始恢复PostgreSQL数据库 ===")
        
        # 如果未指定备份路径，使用最新的备份
        if not backup_path:
            backup_path = self.find_latest_backup()
            if not backup_path:
                print("未找到备份，无法恢复PostgreSQL数据库")
                return
        
        postgres_backup_dir = os.path.join(backup_path, "postgres")
        if not os.path.exists(postgres_backup_dir):
            print(f"PostgreSQL备份目录不存在: {postgres_backup_dir}")
            return
        
        try:
            # 连接到PostgreSQL服务器
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 删除现有数据库
            print("删除现有数据库...")
            databases_to_restore = ["outline", "outline_test", "keycloak"]
            for db_name in databases_to_restore:
                try:
                    cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
                    print(f"已删除数据库: {db_name}")
                except Exception as e:
                    print(f"删除数据库 {db_name} 失败: {e}")
            
            # 创建用户和数据库
            print("创建用户和数据库...")
            try:
                # 创建用户（如果不存在）
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='outline'")
                if not cursor.fetchone():
                    cursor.execute("CREATE USER outline WITH PASSWORD 'kQSHaDep7U3sTyicgp3lnzdfMI0VWe';")
                
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='keycloak'")
                if not cursor.fetchone():
                    cursor.execute("CREATE USER keycloak WITH PASSWORD 'kQSHaDep7U3sTyicgp3lnzdfMI0VWe';")
                
                # 创建数据库
                cursor.execute("CREATE DATABASE outline OWNER outline;")
                cursor.execute("CREATE DATABASE outline_test OWNER outline;")
                cursor.execute("CREATE DATABASE keycloak OWNER keycloak;")
                
                # 授予超级用户权限
                cursor.execute("ALTER USER outline WITH SUPERUSER;")
                
                print("已创建用户和数据库")
            except Exception as e:
                print(f"创建用户和数据库失败: {e}")
            
            cursor.close()
            conn.close()
            
            # 恢复数据
            for db_name in databases_to_restore:
                backup_file = os.path.join(postgres_backup_dir, f"{db_name}_backup.sql")
                if os.path.exists(backup_file):
                    print(f"恢复数据库: {db_name}")
                    
                    # 设置环境变量PGPASSWORD
                    env = os.environ.copy()
                    env["PGPASSWORD"] = self.pg_password
                    
                    # 使用psql恢复数据
                    import subprocess
                    psql_cmd = [
                        "psql", 
                        "-h", self.pg_host,
                        "-p", str(self.pg_port),
                        "-U", self.pg_user,
                        "-d", db_name,
                        "-f", backup_file
                    ]
                    
                    process = subprocess.Popen(
                        psql_cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE,
                        env=env,
                        universal_newlines=True
                    )
                    _, stderr = process.communicate()
                    
                    if process.returncode != 0:
                        print(f"恢复 {db_name} 失败: {stderr}")
                    else:
                        print(f"成功恢复数据库 {db_name}")
                else:
                    print(f"警告: 未找到 {db_name} 的备份文件")
            
            print("PostgreSQL数据库恢复完成")
            
        except Exception as e:
            print(f"PostgreSQL恢复过程中出错: {e}")
    
    def restore_minio(self, backup_path=None):
        """恢复MinIO存储桶"""
        print("\n=== 开始恢复MinIO存储桶 ===")
        
        # 如果未指定备份路径，使用最新的备份
        if not backup_path:
            backup_path = self.find_latest_backup()
            if not backup_path:
                print("未找到备份，无法恢复MinIO存储桶")
                return
        
        minio_backup_dir = os.path.join(backup_path, "minio")
        if not os.path.exists(minio_backup_dir):
            print(f"MinIO备份目录不存在: {minio_backup_dir}")
            return
        
        try:
            # 创建MinIO客户端
            minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            
            # 获取备份的存储桶列表
            buckets = [d for d in os.listdir(minio_backup_dir) 
                      if os.path.isdir(os.path.join(minio_backup_dir, d))]
            
            if not buckets:
                print("未找到MinIO存储桶备份，跳过MinIO恢复")
                return
            
            print(f"找到以下存储桶备份: {', '.join(buckets)}")
            
            # 恢复每个存储桶
            for bucket_name in buckets:
                print(f"恢复存储桶: {bucket_name}")
                bucket_backup_dir = os.path.join(minio_backup_dir, bucket_name)
                
                # 创建桶（如果不存在）
                try:
                    if not minio_client.bucket_exists(bucket_name):
                        minio_client.make_bucket(bucket_name)
                        print(f"已创建存储桶: {bucket_name}")
                except Exception as e:
                    print(f"创建存储桶 {bucket_name} 失败: {e}")
                    continue
                
                # 恢复数据
                for root, dirs, files in os.walk(bucket_backup_dir):
                    for file in files:
                        if file == "policy.json":  # 跳过策略文件
                            continue
                        
                        file_path = os.path.join(root, file)
                        object_name = os.path.relpath(file_path, bucket_backup_dir)
                        
                        try:
                            # 上传文件
                            minio_client.fput_object(
                                bucket_name, object_name, file_path
                            )
                            print(f"已恢复: {object_name}")
                        except Exception as e:
                            print(f"恢复对象 {object_name} 失败: {e}")
                
                # 恢复桶策略
                policy_file = os.path.join(bucket_backup_dir, "policy.json")
                if os.path.exists(policy_file):
                    try:
                        with open(policy_file, 'r') as f:
                            policy = f.read()
                        minio_client.set_bucket_policy(bucket_name, policy)
                        print(f"已恢复 {bucket_name} 的策略")
                    except Exception as e:
                        print(f"恢复 {bucket_name} 的策略失败: {e}")
            
            print("MinIO存储桶恢复完成")
            print("注意：请检查 minio/data/outline 下的内容，确保不是 minio/data/outline/outline 结构")
            
        except Exception as e:
            print(f"MinIO恢复过程中出错: {e}")
    
    def find_latest_backup(self):
        """查找最新的备份目录"""
        if not os.path.exists(self.backup_dir):
            return None
        
        backups = [d for d in os.listdir(self.backup_dir) 
                  if os.path.isdir(os.path.join(self.backup_dir, d)) and d.startswith("backup_")]
        
        if not backups:
            return None
        
        # 按名称排序（因为包含时间戳）
        backups.sort(reverse=True)
        latest_backup = os.path.join(self.backup_dir, backups[0])
        
        print(f"找到最新备份: {latest_backup}")
        return latest_backup
    
    def perform_full_backup(self):
        """执行完整备份"""
        self.backup_postgres()
        self.backup_minio()
        print(f"\n=== 备份完成 ===")
        print(f"备份保存在: {self.current_backup_dir}")
    
    def perform_full_restore(self, backup_path=None):
        """执行完整恢复"""
        if not backup_path:
            backup_path = self.find_latest_backup()
            if not backup_path:
                print("未找到备份，无法执行恢复")
                return
        
        self.restore_postgres(backup_path)
        self.restore_minio(backup_path)
        print(f"\n=== 恢复完成 ===")
        print(f"已从 {backup_path} 恢复数据")

def main():
    parser = argparse.ArgumentParser(description="Outline服务备份恢复工具")
    parser.add_argument("action", choices=["backup", "restore"], help="执行备份或恢复操作")
    parser.add_argument("--backup-dir", default="./backups", help="备份存储目录")
    parser.add_argument("--restore-path", help="恢复时使用的备份路径，默认使用最新备份")
    
    args = parser.parse_args()
    
    backup_tool = OutlineBackupTool(backup_dir=args.backup_dir)
    
    if args.action == "backup":
        backup_tool.perform_full_backup()
    elif args.action == "restore":
        backup_tool.perform_full_restore(args.restore_path)

if __name__ == "__main__":
    main()
