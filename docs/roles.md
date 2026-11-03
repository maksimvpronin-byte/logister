# Roles

Logister has three roles. A grant binds a user or an LDAP group to
one of them, on the whole installation or on one stream. The group is not a
permission. With `AUTH_ENABLED=true`, the HTTP API enforces the contract in
`api/internal/authz` on every request.

## Grants

| Field | Meaning |
| --- | --- |
| Principal kind | `user` or `group` |
| Principal id | Local user id, or the LDAP `entryUUID` |
| Role | `operator`, `service`, or `administrator` |
| Scope | `system` (the whole installation) or `stream` |
| Stream id | Stream primary key. Set only when the scope is `stream` |

Store `entryUUID`, not the group name or DN. A rename in the directory then
leaves the grant in place. The ordinary LDAP adapter reads `inetOrgPerson`
users by `uid` and direct `groupOfNames` memberships through `member`.
Nested groups are not expanded. Users and groups must expose `entryUUID`.
AD `objectSid`/`tokenGroups` are not used by this adapter.

LDAP passwords are verified with a separate user bind and are never stored by
Logister. Browser sessions last eight hours, use HttpOnly/SameSite=Strict cookies,
and store only a token hash in PostgreSQL. Group memberships and grants are
resolved again on every authenticated request, including existing sessions.
Deleted users lose access; an unavailable directory fails closed. A password
change or `{CRYPT}!` lock prevents new sign-ins but does not revoke an already
issued session; remove memberships/grants to revoke its permissions immediately.

Access is the union of matching grants. There is no deny. To remove access,
delete the grant or remove the user from the group.

A system grant of a stream permission covers every stream, including streams
created later. A stream grant applies only to that stream. `administrator`
may only be granted on the system.

The catalog of grants must keep at least one system `administrator`. One of
those grants is a local break-glass user, so an unreachable directory does
not lock the installation. A directory group mapped to `administrator` also
counts. Do not remove the local grant just because the group mapping exists:
an empty group would then be the only administrator.

## Roles

| Role | What it can do | Where the grant points | Typical directory group |
| --- | --- | --- | --- |
| `operator` | Search and open messages. See that stream in the stream list | One stream. A system grant reads every stream | `LOGISTER-<stream>-Operators` |
| `service` | Call the ingest API. No search and no settings | One stream writes only when the request names it. A system grant may use automatic routing and any stream | The shipper account, for example `LOGISTER-Ingest` |
| `administrator` | Every capability: streams, routing, rotation, retention, inputs, grants, search and ingest | Installation only | `LOGISTER-Admins` |

An operator grant on `payments` does not reveal `billing`. The Messages
choice **All messages** is the union of streams that operator may search. It
is not the default stream named `all`. That stream holds unmatched messages
and needs its own grant.

`Streams(messages:search)` returns either "every stream" or an explicit id
list. An empty list means the search returns nothing. Omitting the stream
filter in that case would expose streams the subject cannot read.

`AllowIngest("")` is true only for a system `service` grant, because routing
can select the fallback stream. A stream grant authorizes
`POST /api/streams/{name}/ingest` and a body that names that same stream.

Changing a stream, rotating an index, editing retention and managing grants
belong to `administrator`. `GET /api/health` stays outside this model so
probes do not need a role.

## Permissions

These codes are what the three roles bundle. Directory groups are mapped to
the role, not to a code.

| Code | Held by | Allows |
| --- | --- | --- |
| `messages:search` | operator, administrator | Search and open messages |
| `streams:read` | operator, administrator | See the stream in the list |
| `ingest:write` | service, administrator | Accept messages |
| `streams:edit` | administrator | Change title, description and routing |
| `streams:rotate` | administrator | Seal the active index |
| `streams:retention` | administrator | Change rotation thresholds and retention |
| `streams:create` | administrator | Create a stream |
| `streams:delete` | administrator | Delete a stream. Reserved until that API exists |
| `inputs:read` | administrator | List inputs |
| `system:grants` | administrator | View and change grants, including directory mappings |

`streams:create`, `streams:delete`, `inputs:read` and `system:grants` apply
only to a system grant. The others also apply to the stream named on the
grant.

## What is not in this model

Field-level hiding, per-index grants and explicit denies are not part of the
model. Custom roles, nested group expansion and automatic AD/Samba discovery are not implemented.


## Running the local LDAP integration

The running local stack uses the LDAP lab in `deploy/ldap`. Start or rebuild it
with authentication using:

```sh
sh deploy/start-ldap-auth.sh
# First-time demo setup only; adds payments/billing streams and four group mappings:
python3 deploy/tests/ldap_auth.py --setup
```

Open <http://127.0.0.1:3000>. Directory users have the password `TestUser-123!`.
`administrator` has full access, `operator` reads payments, `billing` reads billing,
`multi` reads both and can ingest, `service` can ingest only, `no-role` can sign in
without receiving access, and `disabled` cannot sign in. Streams and assignments
are created by `--setup`; LDAP group names alone never confer application rights.
The test creates tagged messages in the two demo streams and restores temporary
LDAP membership/name changes.

For emergency access choose **Локальный аварийный администратор**, username
`local-admin`, password `AUTH_LOCAL_PASSWORD` from `deploy/ldap/.env`. This account
works even when LDAP is down; its administrator grant cannot be removed. Its
password is hashed with bcrypt at startup. It is separate from the LDAP server's
`cn=admin` account. The `.env` file is excluded from Git.

Use **Пользователи и роли → Предоставить доступ** to choose a directory user/group, role and
scope, then save that assignment. Existing rows have **Edit** and **Remove**
actions: editing replaces that row immediately after **Сохранить изменения**, and
removal requires confirmation. **Cancel** leaves saved access unchanged.
Duplicate assignments are blocked; the emergency administrator is marked
**Protected**. Search and Groups/Users filters help find existing assignments.
Assignments use UUIDs, persist across
restarts, and reject conflicting saves using a catalog revision. `administrator`
can only be installation-wide. Stream-scoped permissions use stream primary keys.
Search and the stream list are restricted on the server, including totals and
pagination. Cursor/session tokens are bound to the user and current stream scope;
a changed scope invalidates old cursors. Ingestion validates the whole batch before
committing; on authenticated ingestion endpoints, stream-scoped service grants
cannot use implicit routing.

The dedicated `POST /api/ingest/fluent-bit` receiver accepts messages without
authentication or LDAP role checks, including when LDAP is unavailable. The
Fluent Bit output sends no credentials. This receiver may use automatic routing
or explicitly name any configured stream. Existing shipper state/queues remain
in their volumes.

The other ingestion endpoints (`/api/ingest/json` and
`/api/streams/{name}/ingest`) still require an authenticated session or LDAP HTTP
Basic credentials and enforce service/admin grants. To use a scoped service
grant there, set an explicit permitted stream on every event or use the stream
URL. Browser management continues to use sessions.

The local Compose override permits HTTP and plain LDAP on the local Docker
network, with host ports bound to `127.0.0.1`. For a remote deployment use HTTPS,
`AUTH_COOKIE_SECURE=true` (the default), an `ldaps://` directory with a trusted
certificate and the correct exact `CORS_ORIGIN`. The proxy must preserve the
original Host header including port. Cross-origin mutations are rejected;
failed sign-ins are limited to ten per minute per immediate peer address.

The root Compose file alone retains explicit development mode without sign-in.
Use the start script/merged override to preserve authentication when recreating
the local stack. For a direct API process, set:

| Variable | Purpose |
| --- | --- |
| `AUTH_ENABLED=true` | Enforce authentication and roles |
| `AUTH_LOCAL_PASSWORD` | Required emergency password, 16–72 bytes |
| `AUTH_COOKIE_SECURE` | Defaults true; false only for local HTTP |
| `AUTH_SETTINGS_KEY` | Persistent 64-character hex key for LDAP password encryption |
| `LDAP_URL` | Initial `ldap://` or `ldaps://` server (first setup only) |
| `LDAP_BIND_DN`, `LDAP_BIND_PASSWORD` | Initial reader service credentials |
| `LDAP_USERS_DN`, `LDAP_GROUPS_DN` | Initial user/group search bases |
| `CORS_ORIGIN` | Exact browser origin, e.g. `http://localhost:3000` |

Public endpoints: `GET /api/health`, `GET /api/auth/status`, `POST /api/auth/login`,
and `POST /api/ingest/fluent-bit`.
Authenticated endpoints include `GET /api/auth/me` and `POST /api/auth/logout`.
Administrators manage `GET /api/auth/directory?q=...` and `GET/PUT /api/auth/grants`.

## Validation

```sh
cd api
LOGISTER_TEST_DATABASE_URL='postgres://logister:logister@127.0.0.1:5432/logister?sslmode=disable' go test ./...
go vet ./...
cd ../ui
npm test
npm run build
cd ..
python3 deploy/tests/ldap_auth.py
# Optional live outage drill; briefly stops and restores the local LDAP service:
python3 deploy/tests/ldap_auth.py --outage
```

The live checks cover anonymous access, LDAP injection, disabled/empty-password
logins, all six active users, role union, stream isolation, query bypass attempts,
cross-user cursors, immediate membership revocation, stable mappings after rename,
local administrator preservation, CSRF, conflicting updates, and logout. Unit
checks also cover scoped ingestion rejecting whole mixed-permission batches,
expired sessions, login throttling, cursor invalidation and emergency access during
an identity-provider outage. PostgreSQL tests run in isolated transaction schemas.


## LDAP connection settings

Administrators can open **Настройки LDAP** (`/#/ldap`) to configure the server URL,
reader Bind DN/password, and user/group search bases. The page and all three
endpoints (`GET/PUT /api/auth/ldap`, `POST /api/auth/ldap/test`) require the system
administrator role; other authenticated users receive 403.

**Проверить подключение** checks an unsaved draft without applying it. **Сохранить настройки**
rechecks the connection, reader bind, search-base access and representative
`inetOrgPerson`/`groupOfNames` records before saving. Failed checks leave the
previous configuration active. Empty bases are allowed and reported as empty;
records must expose `entryUUID` and the adapter's uid/cn attributes.

Connection settings are persisted in PostgreSQL with revision checks. LDAP
operations read the saved configuration on each request, so changes apply without
a restart and across API instances. Environment variables only initialize the
first stored record; subsequent restarts never overwrite saved settings. LDAP
can also be configured from scratch using the local emergency administrator
without initial LDAP environment values. Existing role mappings are preserved;
when changing to a different directory, review UUID-based assignments there.

The service password is encrypted using AES-256-GCM with `AUTH_SETTINGS_KEY`.
The API returns only a `password_set` flag, never the password or ciphertext.
Leaving the password input empty retains the stored secret; supplying a value
replaces it. The startup script generates the key once in `deploy/ldap/.env`.
Preserve this file/key separately from database backups and provide the same key
to all API instances. If the key is lost or changed, use emergency local access
to enter the service password again. Do not rotate the key by simply replacing it
while retaining the old ciphertext.

Supported transport is `ldap://` or `ldaps://` with normal CA verification; there
is no certificate-verification bypass. Plain LDAP is intended for the local lab.
The URL is resolved from the API container/network, rather than the browser.
