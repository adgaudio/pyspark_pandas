from distutils.core import setup
try:
    from setuptools import find_packages
except ImportError:
    print ("Please install Distutils and setuptools"
           " before installing this package")
    raise

setup(
    name='pyspark-pandas',
    version='0.0.4',
    description=(
        'Tools and algorithms for pandas Dataframes distributed on pyspark.'
        ' Please consider the SparklingPandas project before this one'),
    long_description="Check the project homepage for details",
    keywords=['spark pyspark pandas dataframe series', ],

    author='Alex Gaudio',
    author_email='adgaudio@gmail.com',
    url='https://github.com/adgaudio/pyspark_pandas',

    packages=find_packages(),
    include_package_data=True,
    install_requires = ['pandas'],
    tests_require=['nose'],
    test_suite="nose.main",
)
