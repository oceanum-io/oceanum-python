import requests
import re
from collections.abc import MutableMapping


class ZarrClient(MutableMapping):
    def __init__(self, connection, datasource, method="post"):
        self.datasource = datasource
        self.method = method
        self.headers = connection._auth_headers
        self.gateway = connection._gateway + "/zarr"

    def __getitem__(self, item):
        resp = requests.get(
            f"{self.gateway}/{self.datasource}/{item}", headers=self.headers
        )
        if resp.status_code != 200:
            raise KeyError(item)
        return resp.content

    def __setitem__(self, item, value):
        if self.method == "put":
            requests.put(
                f"{self.gateway}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )
        else:
            requests.post(
                f"{self.gateway}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )

    def __delitem__(self, item):
        requests.delete(
            f"{self.gateway}/{self.datasource}/{item}", headers=self.headers
        )

    def __iter__(self):
        resp = requests.get(f"{self.gateway}/{self.datasource}", headers=self.headers)
        ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
        links = [u[2] for u in ex.findall(resp.text)]
        for link in links:
            yield link

    def __len__(self):
        return 0
