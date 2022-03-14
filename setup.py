import pathlib
from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="raw-sql-automigrate",
    version="0.0.1",
    packages=find_packages("src"),
    package_dir={"": "src"},
    url="",
    license="",
    author="pavivin",
    author_email="pavivin@yandex.ru",
    description="",
    python_requires=">=3.7.*",
)
