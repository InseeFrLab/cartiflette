from cartiflette.api import download_from_cartiflette_inner

print("This is an experimental version of cartiflette published on PyPi.")
print("To use the latest stable version, you can install it directly from GitHub with the following command:")
print("pip install git+https://github.com/inseeFrLab/cartiflette.git")


carti_download = download_from_cartiflette_inner

__all__ = ["carti_download"]
