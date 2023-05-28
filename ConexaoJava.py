import platform

def check_jdk():
    java_version = platform.java_ver()
    if java_version:
        return print("Versão do JDK encontrada:", java_version)
    else:
        return print("JDK não encontrado.")



