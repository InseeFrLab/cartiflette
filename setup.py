import setuptools


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    packages=setuptools.find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_data={'': ['README.md', '*.yaml']},
    include_package_data=True,
    zip_safe=False)