from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="zenithdb",
    version="1.0.2",
    author="jolovicdev",
    author_email="jolovic@pm.me",
    description="SQLite-powered document database with MongoDB-like syntax, full-text search, and advanced querying capabilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jolovicdev/zenithdb",
    packages=find_packages(exclude=["tests*", "docs*"]),  # Exclude test files from distribution
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    python_requires=">=3.7",
    extras_require={
        "dev": [
            "pytest>=7.0.0,<8.0.0",
            "pytest-cov>=4.0.0,<5.0.0",
            "black>=22.0.0,<23.0.0",
            "isort>=5.0.0,<6.0.0",
        ],
        "docs": [
            "sphinx>=4.0.0,<5.0.0",
            "sphinx-rtd-theme>=1.0.0,<2.0.0",
        ],
    },
    keywords=[
        "database",
        "nosql",
        "document-store",
        "sqlite",
        "json",
        "document-database",
    ],
    project_urls={
        "Bug Reports": "https://github.com/jolovicdev/zenithdb/issues",
        "Source": "https://github.com/jolovicdev/zenithdb",
        "Documentation": "https://github.com/jolovicdev/zenithdb/blob/master/README.md",
    },
    package_data={
        "zenithdb": ["py.typed", "*.pyi", "**/*.pyi"],  # Include type hints
    },
)