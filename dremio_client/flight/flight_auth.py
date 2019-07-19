from pyarrow.flight import ClientAuthHandler
from pyarrow.compat import tobytes
import base64


class HttpDremioClientAuthHandler(ClientAuthHandler):

    def __init__(self, username, password):
        ClientAuthHandler.__init__(self)
        self.username = tobytes(username)
        self.password = tobytes(password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        outgoing.write(base64.b64encode(self.username + b':' + self.password))
        self.token = incoming.read()

    def get_token(self):
        return self.token
