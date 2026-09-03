# Deploying on a Linux box (systemd)

1. Copy `jobgpumonitor-server.example.service` to `../jobgpumonitor-server.service`, replace `USER`.
2. Copy `update.example.bat` to `../update.bat`, set `HOST`. Both copies are gitignored.
3. Run `update.bat` from Windows (or the same ssh lines from any shell). It pulls, installs into `.venv`,
   installs the unit and (re)starts the service. The API listens on `127.0.0.1:21834`; put your
   reverse proxy in front of it.
4. Configuration lives in `~/.config/jgm-server/config.toml` on the host (`jgmd init` writes an example).
