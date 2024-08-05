import json



class BaseRenderer:
    def __init__(self, raw_json: str):
        self._raw = raw_json
        self.data = json.loads(raw_json)

    def render(self):
        raise NotImplementedError("You must implement the render method.")