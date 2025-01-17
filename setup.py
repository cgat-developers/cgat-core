from setuptools import setup, find_packages

if __name__ == "__main__":
    setup(
        name="cgatcore",
        packages=find_packages(),
        package_data={
            "cgatcore": ["*.py"],
            "cgatcore.pipeline": ["*.py"],
        },
    )
