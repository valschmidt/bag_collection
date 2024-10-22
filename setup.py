from setuptools import setup, find_packages


setup(
    name = "bag_collection",
    version = "1.0.0",
    license = "CC0 1.0 Universal",
    packages = find_packages(),
    python_requires=">=3.0",
    install_requires=[
        "pandas",
        "numpy",
        "rosbag"
    ],
    description="Module for pulling data from a collection of ROS1 bag files.",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    entry_points={
        'console_scripts': ['bag_collection.py=bag_collection.bag_collection:main']
    },
    keywords = "ROS, bag",
    author = "Val Schmidt",
    author_email="vschmidt@ccom.unh.edu"

)