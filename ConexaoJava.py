import subprocess

def check_java_version():
    try:
        java_version_output = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
        java_version_string = java_version_output.decode('utf-8')
        java_version = java_version_string.split('\n')[0]
        print(f'Java version: {java_version}')
    except subprocess.CalledProcessError as e:
        print('Java is not installed or configured properly.')

check_java_version()
