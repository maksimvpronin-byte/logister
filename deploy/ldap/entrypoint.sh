#!/bin/sh
set -eu
mkdir -p /run/slapd
admin_hash=$(slappasswd -s "$LDAP_ADMIN_PASSWORD")
sed "s|@ADMIN_HASH@|$admin_hash|" /opt/ldap/slapd.conf > /run/slapd/slapd.conf
if [ ! -f /var/lib/ldap/data.mdb ]; then
    user_hash=$(slappasswd -s "$LDAP_TEST_PASSWORD")
    reader_hash=$(slappasswd -s "$LDAP_READER_PASSWORD")
    sed -e "s|@USER_HASH@|$user_hash|g" -e "s|@READER_HASH@|$reader_hash|g" \
        /opt/ldap/seed.ldif > /run/slapd/seed.ldif
    slapadd -f /run/slapd/slapd.conf -l /run/slapd/seed.ldif
    rm /run/slapd/seed.ldif
fi
chown -R openldap:openldap /run/slapd /var/lib/ldap
exec slapd -f /run/slapd/slapd.conf -h ldap://0.0.0.0:389/ -u openldap -g openldap -d 256
