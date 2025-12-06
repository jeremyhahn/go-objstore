"""Setup script for go-objstore-sdk package."""
from setuptools import setup, find_packages

setup(
    name="go-objstore-sdk",
    version="0.1.0",
    description="Python SDK for go-objstore with REST, gRPC, and QUIC/HTTP3 support",
    long_description=open("README.md").read() if __file__ else "",
    long_description_content_type="text/markdown",
    author="Go ObjectStore Team",
    license="AGPL-3.0",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.31.0",
        "grpcio>=1.60.0",
        "grpcio-tools>=1.60.0",
        "protobuf>=4.25.0",
        "httpx[http3]>=0.26.0",
        "pydantic>=2.5.0",
        "tenacity>=8.2.3",
        "typing-extensions>=4.9.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-asyncio>=0.23.2",
            "pytest-mock>=3.12.0",
            "black>=23.12.0",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
            "isort>=5.13.2",
            "responses>=0.24.1",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
