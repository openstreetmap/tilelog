from setuptools import find_packages, setup


setup(
    name='tilelog',
    version='1.4.1',
    author="Paul Norman",
    author_email="osm@paulnorman.ca",
    url="https://github.com/openstreetmap/tilelog",
    packages=find_packages(),
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'Click',
        'publicsuffixlist',
        'pyathena',
        'PyAthena[Arrow]'
    ],
    setup_requires=[
        'flake8'
    ],
    entry_points={
        'console_scripts': ['tilelog=tilelog:cli']
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: GIS",
        "Intended Audience :: Science/Research"
    ],
    python_requires="~=3.6"
)
