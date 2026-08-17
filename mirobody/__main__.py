"""``python -m mirobody`` — same CLI as the ``mirobody`` console script.

Deployments prefer this form: the module lives in site-packages (which
compose.yaml persists in a volume), whereas the console-script shim in the
venv's ``bin/`` does not survive a container rebuild.
"""

from mirobody.cli import main

main()
