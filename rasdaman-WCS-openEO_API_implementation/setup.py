from setuptools import setup, find_packages

setup(
    name="rasdaman-openeo",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'click',
        'rich',
        'streamlit',
        'requests',
        'pandas',
        'plotly'
    ],
    entry_points={
        'console_scripts': [
            'openeo-cli=interface.cli:cli',
        ],
    },
)