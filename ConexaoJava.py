import subprocess

def check_java_version():
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        output = result.stderr.strip()
        if result.returncode == 0 and 'java version' in output:
            version_line = output.splitlines()[0]
            java_version = version_line.split()[-1]
            return print(f"JDK version: {java_version}")
        else:
            return print("JDK not found or command execution failed.")
    except FileNotFoundError:
        return print("Java executable not found.")


check_java_version()
