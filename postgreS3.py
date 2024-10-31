import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import lobject
from typing import Optional, Tuple, BinaryIO, Union
import hashlib
from datetime import datetime
import urllib3
from dataclasses import dataclass
import os
import json

@dataclass
class ClientConfig:
    host: str
    port: int = 5432
    database: str = "postgres"
    user: Optional[str] = None
    password: Optional[str] = None
    region: Optional[str] = None
    secure: bool = True
    http_client: Optional[urllib3.PoolManager] = None
    min_connections: int = 1
    max_connections: int = 10
    chunk_size: int = 8192  # Size of chunks when reading/writing files

class PostgresStorageClient:
    def __init__(
        self,
        endpoint: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        secure: bool = True,
        http_client: Optional[urllib3.PoolManager] = None,
        min_connections: int = 1,
        max_connections: int = 10
    ):
        """
        Initialize PostgreSQL storage client with Minio-like interface.
        
        :param endpoint: Database endpoint (host:port/database)
        :param access_key: Database username
        :param secret_key: Database password
        :param region: Optional region (stored as metadata)
        :param secure: Use SSL connection if True
        :param http_client: Optional HTTP client for connection pooling
        :param min_connections: Minimum number of connections in pool
        :param max_connections: Maximum number of connections in pool
        Uses LARGE OBJECTS for file storage (up to 4TB per file).
        """
        self.config = self._parse_endpoint(endpoint)
        self.config.user = access_key
        self.config.password = secret_key
        self.config.region = region
        self.config.secure = secure
        self.config.http_client = http_client
        self.config.min_connections = min_connections
        self.config.max_connections = max_connections
        
        # Initialize database connection
        self._init_connection()
        
        # Ensure storage table exists
        self._create_storage_table()
    
    def _parse_endpoint(self, endpoint: str) -> ClientConfig:
        """Parse endpoint string into config."""
        parts = endpoint.split(':')
        if len(parts) == 1:
            host = parts[0]
            port = 5432
            database = "postgres"
        else:
            host = parts[0]
            port_db = parts[1].split('/')
            port = int(port_db[0])
            database = port_db[1] if len(port_db) > 1 else "postgres"
        
        return ClientConfig(
            host=host,
            port=port,
            database=database
        )
    
    def _init_connection(self):
        """Initialize database connection or connection pool."""
        try:
            self.pool = SimpleConnectionPool(
                minconn=self.config.min_connections,
                maxconn=self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                sslmode='require' if self.config.secure else 'disable'
            )
            self._use_pool = True
        except (AttributeError, ImportError):
            # Fallback to single connection if pooling is not available
            self.conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                sslmode='require' if self.config.secure else 'disable'
            )
            self._use_pool = False
    
    def _get_conn(self):
        """Get a connection from the pool or return the single connection."""
        if self._use_pool:
            return self.pool.getconn()
        return self.conn
    
    def _put_conn(self, conn):
        """Return a connection to the pool if using pooling."""
        if self._use_pool:
            self.pool.putconn(conn)
    
    def _create_storage_table(self):
        """Create the file storage table if it doesn't exist."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS storage_buckets (
                        bucket_name VARCHAR PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        region VARCHAR,
                        metadata JSONB
                    );
                    
                    CREATE TABLE IF NOT EXISTS storage_objects (
                        bucket_name VARCHAR REFERENCES storage_buckets(bucket_name),
                        object_name VARCHAR,
                        loid OID NOT NULL,  -- Large Object ID
                        size BIGINT NOT NULL,
                        etag VARCHAR,
                        content_type VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        metadata JSONB,
                        PRIMARY KEY (bucket_name, object_name)
                    );
                """)
                conn.commit()
        finally:
            self._put_conn(conn)
            
    def make_bucket(self, bucket_name: str, location: Optional[str] = None) -> None:
        """
        Create a new bucket.
        
        :param bucket_name: Name of the bucket
        :param location: Optional region/location for the bucket
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO storage_buckets (bucket_name, region)
                    VALUES (%s, %s)
                    """,
                    (bucket_name, location or self.config.region)
                )
                conn.commit()
        finally:
            self._put_conn(conn)
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM storage_buckets WHERE bucket_name = %s",
                    (bucket_name,)
                )
                return cur.fetchone() is not None
        finally:
            self._put_conn(conn)
    
    def remove_bucket(self, bucket_name: str) -> None:
        """Remove a bucket."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM storage_buckets WHERE bucket_name = %s",
                    (bucket_name,)
                )
                conn.commit()
        finally:
            self._put_conn(conn)
    
    def fput_object(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> dict:
        """
        Upload a file to a bucket using LARGE OBJECTS.
        Supports files up to 4TB in size.
        """
        if not self.bucket_exists(bucket_name):
            raise ValueError(f"Bucket '{bucket_name}' does not exist")
        
        file_size = os.path.getsize(file_path)
        md5_hash = hashlib.md5()
        
        conn = self._get_conn()
        try:
            # Create a new large object
            lobj = conn.lobject(mode='wb')
            loid = lobj.oid
            
            # Read and write file in chunks to calculate MD5 and save to large object
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(self.config.chunk_size)
                    if not chunk:
                        break
                    md5_hash.update(chunk)
                    lobj.write(chunk)
            
            etag = md5_hash.hexdigest()
            
            # Save the reference in our objects table
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO storage_objects 
                    (bucket_name, object_name, loid, size, etag, content_type, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (bucket_name, object_name) DO UPDATE
                    SET loid = EXCLUDED.loid,
                        size = EXCLUDED.size,
                        etag = EXCLUDED.etag,
                        content_type = EXCLUDED.content_type,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        bucket_name,
                        object_name,
                        loid,
                        file_size,
                        etag,
                        content_type or 'application/octet-stream',
                        json.dumps(metadata) if metadata else None
                    )
                )
                conn.commit()
            
            return {
                "etag": etag,
                "version_id": None
            }
        finally:
            self._put_conn(conn)
    
    def fget_object(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str
    ) -> dict:
        """Download an object from a bucket using LARGE OBJECTS."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT loid, etag, content_type, metadata
                    FROM storage_objects
                    WHERE bucket_name = %s AND object_name = %s
                    """,
                    (bucket_name, object_name)
                )
                result = cur.fetchone()
                
                if result is None:
                    raise ValueError(f"Object '{object_name}' not found in bucket '{bucket_name}'")
                
                loid, etag, content_type, metadata = result
                
                # Open the large object for reading
                lobj = conn.lobject(oid=loid, mode='rb')
                
                # Write to file in chunks
                with open(file_path, 'wb') as f:
                    while True:
                        chunk = lobj.read(self.config.chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                
                return {
                    "etag": etag,
                    "content_type": content_type,
                    "metadata": json.loads(metadata) if metadata else {},
                    "version_id": None
                }
        finally:
            self._put_conn(conn)
    
    def remove_object(self, bucket_name: str, object_name: str) -> None:
        """Remove an object and its associated large object from a bucket."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                # First get the large object ID
                cur.execute(
                    """
                    SELECT loid FROM storage_objects
                    WHERE bucket_name = %s AND object_name = %s
                    """,
                    (bucket_name, object_name)
                )
                result = cur.fetchone()
                
                if result:
                    loid = result[0]
                    # Delete the large object
                    lobj = conn.lobject(oid=loid)
                    lobj.unlink()
                    
                    # Delete the reference
                    cur.execute(
                        """
                        DELETE FROM storage_objects
                        WHERE bucket_name = %s AND object_name = %s
                        """,
                        (bucket_name, object_name)
                    )
                    conn.commit()
        finally:
            self._put_conn(conn)

    def list_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        recursive: bool = False
    ):
        """List objects in a bucket."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                query = """
                    SELECT object_name, size, etag, content_type, created_at
                    FROM storage_objects
                    WHERE bucket_name = %s
                """
                params = [bucket_name]
                
                if prefix:
                    query += " AND object_name LIKE %s"
                    params.append(f"{prefix}%")
                
                if not recursive:
                    query += """ 
                        AND object_name NOT LIKE %s 
                        AND object_name NOT LIKE %s
                    """
                    params.extend([f"{prefix}_%/%", f"_%/%"])
                
                cur.execute(query, params)
                
                for row in cur.fetchall():
                    yield {
                        "object_name": row[0],
                        "size": row[1],
                        "etag": row[2],
                        "content_type": row[3],
                        "last_modified": row[4]
                    }
        finally:
            self._put_conn(conn)

    def __del__(self):
        """Cleanup connections on deletion."""
        if hasattr(self, '_use_pool') and self._use_pool:
            self.pool.closeall()
        elif hasattr(self, 'conn'):
            self.conn.close()