#!/usr/bin/env bash
set -eux
sudo -u postgres dropdb niks3 || true
sudo -u postgres createdb niks3 -O "$USER"
