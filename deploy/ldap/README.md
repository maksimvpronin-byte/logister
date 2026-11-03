# Локальный LDAP-стенд для ролевой модели

OpenLDAP и LDAP Account Manager, отдельный Compose-проект `logister-ldap`.
Работает на Linux/ARM64 и AMD64, Windows не требуется. Пользователи и группы
хранятся в Docker volume и сохраняются после перезапуска.

## Запуск и остановка

Из корня репозитория:

```sh
# Только при первом развёртывании на другой машине:
cp deploy/ldap/.env.example deploy/ldap/.env
# При желании измените пароли в .env перед первым запуском.
docker compose -f deploy/ldap/docker-compose.yml up --build -d
docker compose -f deploy/ldap/docker-compose.yml ps

# Остановить с сохранением каталога:
docker compose -f deploy/ldap/docker-compose.yml down
```

На текущей машине `.env` уже создан: пароли администратора и учётки чтения
случайные; файл исключён из Git и доступен только владельцу. Не перезаписывайте его
примером. `LDAP_READER_PASSWORD` и `LDAP_TEST_PASSWORD` применяются к данным только
при первом создании каталога; дальнейшая смена этих переменных не меняет пароли
существующих LDAP-записей. `LDAP_ADMIN_PASSWORD` применяется при старте сервера.

Стенд использует обычный LDAP без TLS и опубликован только на `127.0.0.1`.
Это локальная тестовая конфигурация.

## Подключение вашего ПО

| Настройка | Значение |
| --- | --- |
| URL с этой машины | `ldap://127.0.0.1:1389` |
| URL из контейнера в сети `logister-ldap_default` | `ldap://ldap:389` |
| Base DN | `dc=logister,dc=test` |
| Bind DN для чтения | `uid=ldap-reader,ou=services,dc=logister,dc=test` |
| Bind password | `LDAP_READER_PASSWORD` из `.env` |
| User search base | `ou=people,dc=logister,dc=test` |
| User filter | `(&(objectClass=inetOrgPerson)(uid={username}))` |
| Login attribute | `uid` |
| Group search base | `ou=groups,dc=logister,dc=test` |
| Group filter | `(&(objectClass=groupOfNames)(member={userDN}))` |
| Group name | `cn` |
| Group members | `member` — полные DN пользователей |
| Groups on user | `memberOf` — запросить явно, это operational attribute |
| Stable record ID | `entryUUID` — запросить явно |

`{username}` и `{userDN}` — обозначения для настройки адаптера, а не буквальные
LDAP-переменные. Экранируйте значения при подстановке в LDAP-фильтры.
После поиска пользователя проверяйте его пароль отдельным bind под найденным DN.
`127.0.0.1` внутри другого контейнера указывает на сам контейнер, не на этот стенд.

Учётка чтения может искать записи, но не менять группы и не читать хеши паролей.
Анонимный поиск закрыт. Настроены `memberOf` и ссылочная целостность `member`.
Вложенные группы автоматически в транзитивный список `memberOf` не разворачиваются.

## Веб-интерфейс

Откройте <http://127.0.0.1:8088>. Выберите `admin`; пароль —
`LDAP_ADMIN_PASSWORD` из `.env`. Полный bind DN администратора:
`cn=admin,dc=logister,dc=test`.

Пользователи доступны в разделе **Users**. Для групп откройте меню **Tools →
Tree view**, дважды щёлкните `ou=groups`, выберите группу и отредактируйте атрибут
`member`. В нём хранятся полные DN, например
`uid=operator,ou=people,dc=logister,dc=test`. Кнопки `+`/`−` рядом со значениями
добавляют и удаляют членов; затем нажмите **Save**. Новая группа создаётся через
**Create a child entry** на `ou=groups` с классом `groupOfNames`, `cn` и `member`.
Такое редактирование проверено в бесплатной версии LAM через Tree view;
обычная вкладка POSIX groups для этого формата не используется.

`groupOfNames` требует хотя бы одного `member`. Для пустой группы используйте
`cn=empty,ou=services,dc=logister,dc=test` — это служебная запись без возможности входа.
Не редактируйте `memberOf` вручную: сервер обновляет его при изменениях `member`.

## Тестовые пользователи и группы

Пароль активных пользователей: `TestUser-123!` (значение `LDAP_TEST_PASSWORD`
при первом запуске). DN пользователя: `uid=<login>,ou=people,dc=logister,dc=test`.

| Login | Группы / назначение |
| --- | --- |
| `operator` | `LOGISTER-payments-Operators` |
| `billing` | `LOGISTER-billing-Operators` |
| `service` | `LOGISTER-Ingest` |
| `administrator` | `LOGISTER-Admins` |
| `multi` | Обе группы Operators и Ingest |
| `no-role` | Нет групп: вход успешен, доступ не должен появиться |
| `disabled` | Admins, но пароль заблокирован: bind отклоняется |

`disabled` использует невалидный хеш `{CRYPT}!`; это тест запрета парольного входа,
а не AD `userAccountControl` или политика блокировки после неудачных попыток.

Пример сопоставления, которое предстоит настроить в ПО:

| Группа | Роль | Область |
| --- | --- | --- |
| `LOGISTER-payments-Operators` | `operator` | stream `payments` |
| `LOGISTER-billing-Operators` | `operator` | stream `billing` |
| `LOGISTER-Ingest` | `service` | system |
| `LOGISTER-Admins` | `administrator` | system |

Имена streams здесь — сценарий теста, сам стенд не создаёт streams в приложении.

## Проверка

```sh
python3 deploy/ldap/verify.py
```

Проверяет вход шести пользователей, отказ заблокированному пользователю,
неверный и пустой пароль, четыре группы, ожидаемое членство, `entryUUID`,
недоступность хешей и запрет изменения групп для reader. Временно добавляет
`no-role` в группу, проверяет `memberOf`, удаляет членство и проверяет отзыв.
Проверка ожидает исходное тестовое наполнение.

## Интеграция с Logister

LDAP-вход и проверка ролей подключены к HTTP API. Чтобы запустить приложение
с авторизацией, из корня репозитория выполните `sh deploy/start-ldap-auth.sh`.
Первичная настройка демонстрационных групп/потоков:
`python3 deploy/tests/ldap_auth.py --setup`.
Подробности и сценарии проверки — в [docs/roles.md](../../docs/roles.md).

Вход в Logister: <http://127.0.0.1:3000>. Аварийный локальный администратор:
выбрать Local emergency administrator, логин `local-admin`, пароль
`AUTH_LOCAL_PASSWORD` из `.env`. Это другая учётка, чем LDAP `cn=admin`.
Администраторы меняют подключение через **LDAP settings** в Logister.
Переменные LDAP в `.env`/Compose служат только начальным заполнением;
сохранённые через интерфейс параметры имеют приоритет.
`AUTH_SETTINGS_KEY` генерируется скриптом запуска один раз; сохраняйте его
для расшифровки пароля учётки чтения после восстановления базы.

Документация компонентов:
[OpenLDAP overlays](https://www.openldap.org/doc/admin26/overlays.html),
[LDAP Account Manager](https://www.ldap-account-manager.org/static/doc/manual/).
