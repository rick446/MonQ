from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='MonQ',
      version=version,
      description="Simple queue package for MongoDB",
      long_description="""MonQ provides a simple queueing solution for MongoDB
      for when you don't want to bother with Redis, Celery, or other solutions.
     """,
      classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
        ],
      keywords='mongodb, queue',
      author='Rick Copeland',
      author_email='rick@arborian.com',
      url='http://blog.pythonisito.com',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'Ming'
      ],
      scripts=[
        'scripts/monq'
        ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
