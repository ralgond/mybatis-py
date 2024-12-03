from setuptools import setup, find_packages

setup(
    name='mybatis',
    version='0.0.1',
    description='A python ORM like mybatis.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',  # 如果你使用的是Markdown格式的README
    author='Teng Huang',
    author_email='ht201509@163.com',
    url='https://github.com/ralgond/mybatis-py',
    packages=find_packages(),
    install_requires=[
        'mysql-connector-python>=9.0.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    # entry_points={
    #     'console_scripts': [
    #         'my-cli=my_project.cli:main',
    #     ],
    # },
    # include_package_data=True,
    # package_data={
    #     'my_project': ['data/*.csv'],
    # },
    license='Apache-2.0',
    test_require=['pytest'],
    tests_require=['pytest'],
)