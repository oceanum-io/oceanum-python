import base64
import time
from typing import Literal
from pathlib import Path
from datetime import datetime

from typing import ClassVar
from typing_extensions import Self
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
    access_token: str
    id_token: str|None = None
    refresh_token: str|None = None
    scope: str|None = None
    domain: str = 'oceanum.tech'
    expires_in: int
    token_type: str
    created_at: datetime = Field(default_factory=datetime.now)

    @classmethod
    def _get_path(cls, domain: str) -> Path:
        domain_slug = domain.replace('.','-')
        return utils.USER_DATA_DIR / f'token-{domain_slug}.json'
    
    @classmethod
    def load(cls, domain: str) -> Self|None:
        token_path = cls._get_path(domain)
        if token_path.exists():
            with token_path.open() as f:
                return cls(**json.load(f))
    
    @property
    def path(self) -> Path:
        return self._get_path(self.domain)

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
        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('w') as f:
            f.write(self.model_dump_json())

    def delete(self) -> bool:
        if self.path.exists():
            self.path.unlink()
            return True
        return False

class Auth0Config(BaseModel):
    domain: str
    client_id: str

class ContextObject(BaseModel):
    domain: str
    token: TokenResponse|None=None
    auth0: Auth0Config|None=None
    