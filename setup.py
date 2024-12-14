from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="zenithdb",
    version="1.0.1",
    author="jolovicdev",
    author_email="jolovic@pm.me",
    description="SQLite-powered document database with MongoDB-like syntax, full-text search, and advanced querying capabilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jolovicdev/zenithdb",
    packages=find_packages(),
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
    install_requires=[],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "isort>=5.0.0",
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
) 