from bs4 import BeautifulSoup
from typing import Optional
import requests
from functools import wraps
from time import time
import os


download_folder = "covid19_csv"
os.makedirs(download_folder, exist_ok=True)  # Create the folder if it doesn't exist



def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %2.4f sec' % \
          (f.__name__, args, kw, te-ts))
        return result
    return wrap

URLS = [
    "https://www.saude.pr.gov.br/Pagina/Boletim-COVID19-2020",
    "https://www.saude.pr.gov.br/Pagina/Boletim-COVID19-2021",
    "https://www.saude.pr.gov.br/Pagina/Boletim-COVID19-2022"
]

class HttpProvider:
    def __init__(self):
        pass

    def get(self, url: str, params: dict = None, headers: dict = None):
        return requests.get(url, params=params, headers=headers)


def batch_download_files(urls: list[str]) -> None:
    """
    Download a list of files from the specified URLs.

    Parameters:
        urls (list[str]): The URLs of the files to download.

    Returns:
        None

    """

    for url in urls:
        downaload_file(url)



def downaload_file(url: str) -> Optional[bytes]:
    """
    Download a file from the specified URL.

    Parameters:
        url (str): The URL of the file to download.

    Returns:
        Optional[bytes]: The content of the downloaded file as bytes, or None if the download fails.

    """

    try:
        file = HttpProvider().get(url).content

        with open(os.path.join(download_folder, build_file_name(url)), "wb") as f:
            f.write(file)
        

    except Exception as e:
        print(f"Error downloading file from {url}: {e}")
        return None
    

def build_file_name(url: str) -> str:
    """
    Extract the file name from the specified URL.

    Parameters:
        url (str): The URL of the file.

    Returns:
        str: The file name.

    """

    return url.split("/")[-1]


def get_soup(url: str) -> BeautifulSoup:
    """
    Get the BeautifulSoup object from the specified URL.

    Parameters:
        url (str): The URL of the file.

    Returns:
        BeautifulSoup: The BeautifulSoup object.

    """

    return BeautifulSoup(HttpProvider().get(url).content, "html.parser")



def search_for_epidemology_files(soup: BeautifulSoup) -> list[str]:
    """
    Search for the epidemiology files in the specified BeautifulSoup object.

    Parameters:
        soup (BeautifulSoup): The BeautifulSoup object.

    Returns:
        list[str]: The URLs of the epidemiology files.

    """

    return [a["href"] for a in soup.find_all("a", href=True) if "Geral" in a.text and ".csv" in a["href"]]


@timing
def scrape_epidemology_files(parse_urls: list[str]) -> None:
    """
    Download the epidemiology files from the specified URLs.

    Parameters:
        urls (list[str]): The URLs of the epidemiology files.

    Returns:
        None

    """

    epidemiology_files_url = []

    for url in parse_urls:
        soup = get_soup(url)
        epidemiology_files_url.extend(search_for_epidemology_files(soup))
    
    batch_download_files(epidemiology_files_url)

print("Downloading files...")
# scrape_epidemology_files(URLS)
print("Download complete.")
