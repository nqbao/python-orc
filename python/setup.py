from setuptools import setup, find_packages
from setuptools.command.install import install

def __read_requirement():
    with open('requirements.txt') as f:
        return f.readlines()

setup(
    cmdclass={'install': install},
    name='python-orc',
    version="0.0.1",
    keywords='Python ORC Reader',
    author='Bao Nguyen',
    author_email='b@nqbao.com',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    scripts=[
        "orc2csv"
    ],
    url='https://github.com/nqbao/python-orc',
    description='Poor man Python ORC Reader',
    install_requires=__read_requirement(),
)