def list_raw_files_level(fs, path_bucket, borders):
    """
    Lists raw files at a specific level within the file system.

    Parameters
    ----------
    fs : FileSystem
        The file system object.
    path_bucket : str
        The path to the bucket in the file system.
    borders : str
        The specific level for which raw files are to be listed.

    Returns
    -------
    list
        A list of raw files at the specified level in the file system.
    """
    list_raw_files = fs.ls(path_bucket)
    list_raw_files = [
        chemin
        for chemin in list_raw_files
        if chemin.rsplit("/", maxsplit=1)[-1].startswith(f"{borders}.")
    ]
    return list_raw_files


def download_files_from_list(fs, list_raw_files, local_dir="temp"):
    """
    Downloads files from a list of raw files to a specified local directory.

    Parameters
    ----------
    fs : FileSystem
        The file system object.
    list_raw_files : list
        A list of raw files to be downloaded.
    local_dir : str, optional
        The local directory where the files will be downloaded, by default "temp".

    Returns
    -------
    str
        The path of the local directory where the files are downloaded.
    """
    for files in list_raw_files:
        fs.download(files, f"{local_dir}/{files.rsplit('/', maxsplit=1)[-1]}")
    return local_dir
