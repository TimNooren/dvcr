from setuptools import setup


setup(
    name="dvcr",
    version="0.4.0",
    description="Containers to use in tests",
    url="http://github.com/TimNooren/dvcr",
    author="Tim Nooren",
    author_email="timnooren@gmail.com",
    license="MIT",
    packages=["dvcr", "dvcr.containers"],
    zip_safe=False,
    install_requires=["docker", "colorama", "pyyaml"],
    tests_require=[
        "cassandra-driver",
        "kafka-python",
        "mysql-connector-python",
        "psycopg2",
        "redis",
        "vertica_python",
    ],
)
