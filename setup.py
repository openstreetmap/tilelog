from setuptools import find_packages, setup


setup(
    name='tilelogs',
    version='0.0.1',
    author="Paul Norman",
    author_email="osm@paulnorman.ca",
    url="https://github.com/openstreetmap/tilelog",
    packages=find_packages(),
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'Click',
        'boto3'
    ],
    setup_requires=[
        'flake8'
    ],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: GIS"
    ],
    python_requires="~=3.6"
)
