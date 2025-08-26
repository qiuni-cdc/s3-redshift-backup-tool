import sys
import os

# Add the project root to the Python path to allow imports from 'src'
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.config.settings import S3Config

print("--- Debugging S3Config Loading ---")
print(f"Current Working Directory: {os.getcwd()}")
print(f"Checking for .env file existence: {os.path.exists('.env')}")

try:
    print("\nAttempting to create S3Config object...")
    # Pydantic's BaseSettings should automatically load from .env
    s3_config = S3Config()
    print("✅ S3Config object created successfully!")
    # Note: Accessing secret_key directly might show '**********'
    print(f"   Access Key: {s3_config.access_key}")
    print(f"   Secret Key: {'*' * 10 if s3_config.secret_key else 'Not Set'}")
    print(f"   Bucket Name: {s3_config.bucket_name}")

except Exception as e:
    print(f"❌ FAILED to create S3Config object.")
    print(f"   Error Type: {type(e).__name__}")
    print(f"   Error Details: {e}")

print("--- End of Debugging Script ---")