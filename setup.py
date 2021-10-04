from setuptools import setup

with open('requirements.txt') as fp:
    install_requires = fp.read().splitlines()

setup(
    name = "mongo_copy_pseudonymize",
    version = "0.0.1",
    author = "Fave",
    author_email = "sergia@faveforfans.com",
    description = ("Copy a mongodb database and replace sensitive fields with pseudonymized values"),
    license = "CC0 1.0 Universal",
    keywords = "mongodb mongo",
    url = "https://github.com/fave-for-fans/mongo-copy-pseudonymize",
    packages=['mongo_pseudonymize'],
    classifiers=[
        "Topic :: Utilities",
    ],
    install_requires=install_requires
)

