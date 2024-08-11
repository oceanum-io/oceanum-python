import base64
import time
from pathlib import Path
from datetime import datetime

from typing import ClassVar, Self
import json

from pydantic import BaseModel, Field

from . import utils


class DeviceCodeResponse(BaseModel):
    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int
    verification_uri_complete: str

class TokenResponse(BaseModel):
    _path: ClassVar[Path] = utils.USER_DATA_DIR / 'token.json'
    access_token: str
    id_token: str|None = None
    refresh_token: str|None = None
    scope: str|None = None
    expires_in: int
    token_type: str
    created_at: datetime = Field(default_factory=datetime.now)
    
    @classmethod
    def load(cls) -> Self|None:
        if cls._path.exists():
            with cls._path.open() as f:
                return cls(**json.load(f))
            
    @property
    def active_org(self) -> str|None:
        if self.access_token is not None:
            payload = base64.b64decode(self.access_token.split('.')[1]+'==').decode('utf-8')
            payload_dict = json.loads(payload)
            return payload_dict.get('https://oceanum.io/active_org', None)
        
    @property
    def email(self) -> str|None:
        if self.access_token is not None:
            payload = base64.b64decode(self.access_token.split('.')[1]+'==').decode('utf-8')
            payload_dict = json.loads(payload)
            return payload_dict.get('https://oceanum.io/email', None)
        
    @property
    def is_expired(self) -> bool:
        return self.created_at.timestamp() + self.expires_in < time.time()
                
    def save(self) -> None:
        if not self._path.parent.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open('w') as f:
            f.write(self.model_dump_json())

    def delete(self) -> bool:
        if self._path.exists():
            self._path.unlink()
            return True
        return False

class Auth0Config(BaseModel):
    domain: str
    client_id: str

class ContextObject(BaseModel):
    domain: str
    token: TokenResponse|None=None
    auth0: Auth0Config|None=None
    