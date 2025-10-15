# Marks 'app' as a package for imports; no runtime side effects recommended.

# Ensures app is a package and exposes a factory for the platform.
from .agent import app as _app

def create_app():
    # The platform imports this to get the graph application.
    return _app

