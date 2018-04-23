import setuptools

setuptools.setup(
    name='duplicate_detector',
    description='tools to detect and deal with duplicate files',
    packages=setuptools.find_packages(),
    # TODO: use less specific versions
    install_requires=[
        'progressbar==2.3',
        'SQLAlchemy==1.0.12',
        'pyxdg==0.25',
    ]
)
