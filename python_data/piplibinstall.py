import subprocess

def install_libraries():
    libraries = [
        'pandas',
        'PyYAML',
        'requests',
        'boto3',
        'mysqldb',
        'sqlalchemy',
        'psycopg2-binary',
        'tabula-py',
        'install-jdk',
        'JPype1',
        #'dask',
        #'cuDF',
        #'NVTabular',
        'apache-airflow',
        'virtualenv',
        'kubernetes'
        ]

    for library in libraries:
        print(f"Installing {library}...")
        subprocess.run(['pip', 'install', library], check=True)
        print(f"{library} installed successfully!")

if __name__ == "__main__":
    install_libraries()

