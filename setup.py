from setuptools import setup, find_namespace_packages

VERSION = "2.1.1"
DESCRIPTION = "Class Wrapper for sqlalchemy, pandas and mongo"
LONG_DESCRIPTION = ""
install_requires = open("requirements.txt").read().strip().split("\n")
# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="DataEngine",
    version=VERSION,
    author="tlibs",
    author_email="<youremail@email.com>",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_namespace_packages(where='src'),
    package_dir={"": "src"},
    install_requires=install_requires,  # add any additional packages that
    # needs to be installed along with your package. Eg: 'caer'
    python_requires=">=3.10",
    keywords=["python", "sqlalchemy", "pandas", "mongo"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: Microsoft :: Windows",
    ],
)


#pip install -U git+https://consumers-checkbook@dev.azure.com/consumers-checkbook/DataEngine/_git/DataEngine@master