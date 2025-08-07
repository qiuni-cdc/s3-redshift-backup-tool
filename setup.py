from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="s3-redshift-backup",
    version="1.0.0",
    author="Development Team",
    author_email="dev@company.com",
    description="S3 to Redshift Incremental Backup System",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/company/s3-redshift-backup",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "s3-backup=cli.main:cli",
        ],
    },
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0", 
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "test": [
            "testcontainers>=3.0.0",
            "pytest-benchmark>=4.0.0",
        ],
    },
)