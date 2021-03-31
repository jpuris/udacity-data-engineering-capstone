import logging
import os

import requests


def check_create_dir(_dir: str) -> str:
    """Creates the specified directory.
    If relative directory path given, current directory path will be prefixed.
    If directory exists, it will not be recreated.

    Missing directories in path will be created recursively.

    Args:
        _dir (str): relative or absolute directory path to be created

    Returns:
        str: Full path of the checked and / or created directory
    """

    # Check if given path is relatve or absolute
    # Relative path does not start with /
    if _dir[0] != '/':
        # Create absolute path
        dir_path = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__),
            ), _dir,
        )
    else:
        dir_path = _dir
    logging.info('Output directory set to: "%s"', dir_path)

    # Check if directory already exists
    if not os.path.isdir(dir_path):
        # Directory does not exist
        logging.info('Output directory does not exist, and will be created')
        # Create directory
        os.makedirs(dir_path)
        logging.info('Output directory created')
    else:
        logging.info('The directory already exists')

    return dir_path


def download_file(url: str, file_path: str, url_params: str = None):
    """Downloads a file from given URL and saves it in given file_path

    Args:
        url (str): File URL for download
        file_path (str): File path on system to put the downloaded file
        url_params (str, optional): URL Params, if any. Defaults to None.
    """

    logging.info('Loading data from "%s", params %s', url, url_params)
    response = requests.get(
        url,
        params=url_params,
        allow_redirects=True,
        stream=True,
    )

    if response.status_code == 200:
        logging.info('Response is 200, saving data to "%s"', file_path)
        with open(os.path.join(file_path), 'wb') as _file:
            _file.write(response.content)
            logging.info('Done saving data file on disk')


def main():
    """Downloads and saves the US Demographics data set"""

    logging.basicConfig(level=logging.INFO)

    # Check and/or create the target directory
    output_directory = check_create_dir('../data')

    download_file(
        'https://public.opendatasoft.com/explore/' +
        'dataset/us-cities-demographics/download',
        os.path.join(output_directory, 'us_demographic_2015.json'),
        url_params={'format': 'json'},
    )


if __name__ == '__main__':
    main()
