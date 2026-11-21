@echo off
REM Reconnecting SSH tunnel: exposes the upstream memory service on 127.0.0.1:3112.
REM The hub is the only writer; the upstream is a passive replica.
REM Requires key-based auth (no password prompt, so the loop can restart silently).
setlocal
set UPSTREAM_USER=myuser
set UPSTREAM_HOST=example-host
set LOCAL_PORT=3112
set REMOTE_PORT=3111

:loop
ssh -N -o ServerAliveInterval=20 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes ^
    -L %LOCAL_PORT%:127.0.0.1:%REMOTE_PORT% %UPSTREAM_USER%@%UPSTREAM_HOST%
REM Connection reset by peer and similar errors exit here; wait, then retry.
timeout /t 10 /nobreak >nul
goto loop
