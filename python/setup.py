from setuptools import setup, find_packages
from setuptools.command.install import install


def __read_requirement():
    with open('requirements.txt') as f:
        return f.readlines()

VERSION = '0.0.1'

setup(
    cmdclass={'install': install},
    name='python-orc',
    version=VERSION,
    keywords='Python ORC Reader',
    author='Bao Nguyen',
    author_email='b@nqbao.com',
    packages=find_packages(exclude=['tests']),
    data_files=[("share/python-orc", ['../java-gateway/target/gateway-%s-jar-with-dependencies.jar' % VERSION])],
    include_package_data=True,
    zip_safe=False,
    scripts=[
        "orc2csv"
    ],
    license="BSD License",
    url='https://github.com/nqbao/python-orc',
    description='Poor man Python ORC Reader',
    install_requires=__read_requirement(),
)
