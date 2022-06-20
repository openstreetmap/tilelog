from setuptools import find_packages, setup


setup(
    name='tilelog',
    version='0.4.0',
    author="Paul Norman",
    author_email="osm@paulnorman.ca",
    url="https://github.com/openstreetmap/tilelog",
    packages=find_packages(),
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'Click',
        'pyathena'
    ],
    setup_requires=[
        'flake8'
    ],
    entry_points={
        'console_scripts': ['tilelog=tilelog:cli']
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: GIS"
    ],
    python_requires="~=3.6"
)
