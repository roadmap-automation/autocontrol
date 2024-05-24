from setuptools import setup

setup(
    name='autocontrol',
    version='0.1',
    packages=['autocontrol'],
    url='https://github.com/criosx/autocontrol',
    license='MIT License',
    author='Frank Heinrich',
    author_email='fheinric@andrew.cmu.edu',
    description='Autocontol task scheduler',
    requires=[
        "numpy", "requests", "flask", "werkzeug", "sqlalchemy", "streamlit", "pandas", "graphviz", "pydantic"
    ]
)
