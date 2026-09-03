@echo off
:: Deploy to a Linux host over SSH (edit HOST). update.bat itself is gitignored: copy this file.
set HOST=USER@your-host.example
ssh %HOST% "cd jobgpumonitor-server && git pull"
ssh %HOST% "sudo systemctl stop jobgpumonitor-server"
ssh %HOST% "cd jobgpumonitor-server && [ ! -d .venv ] && python3 -m venv .venv"
ssh %HOST% "cd jobgpumonitor-server && source .venv/bin/activate && pip install --upgrade pip && pip install -e '.[api,toml]'"
scp jobgpumonitor-server.service %HOST%:jobgpumonitor-server/
ssh %HOST% "sudo cp jobgpumonitor-server/jobgpumonitor-server.service /etc/systemd/system/"
ssh %HOST% "sudo systemctl enable jobgpumonitor-server && sudo systemctl daemon-reload && sudo systemctl start jobgpumonitor-server"
ssh %HOST% "sleep 2 && systemctl is-active jobgpumonitor-server && curl -s http://127.0.0.1:21834/health"
