#!/bin/sh
set -eu
config=/var/lib/ldap-account-manager/config
if [ ! -f "$config/.logister-configured" ]; then
    cp /opt/logister/lam.conf "$config/lam.conf"
    chown www-data:www-data "$config/lam.conf"
    touch "$config/.logister-configured"
fi
exec /usr/local/bin/start.sh
