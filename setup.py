import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="husfort",
    version="1.0.0",
    author="huxiaoou",
    author_email="516984451@qq.com",
    description="Handy & Utility Solution For Operation, Research and Trading",
    packages=setuptools.find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/huxiaoou/husfort",
    install_requires=["numpy", "pandas", "matplotlib", "scipy", "loguru", "rich"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)
