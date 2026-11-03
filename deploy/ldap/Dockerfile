FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d \
    && apt-get update \
    && apt-get install -y --no-install-recommends slapd ldap-utils ca-certificates \
    && rm -rf /var/lib/apt/lists/* /var/lib/ldap/*
COPY slapd.conf /opt/ldap/slapd.conf
COPY seed.ldif /opt/ldap/seed.ldif
COPY entrypoint.sh /usr/local/bin/ldap-entrypoint
RUN chmod +x /usr/local/bin/ldap-entrypoint
EXPOSE 389
ENTRYPOINT ["ldap-entrypoint"]
