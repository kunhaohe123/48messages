#!/usr/bin/env bash
set -euo pipefail

mysql -uroot -p'kunhaohe-123' -D 48pocket -e "SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS message_payloads; DROP TABLE IF EXISTS messages; DROP TABLE IF EXISTS crawl_tasks; DROP TABLE IF EXISTS crawl_checkpoints; DROP TABLE IF EXISTS members; DROP TABLE IF EXISTS rooms; SET FOREIGN_KEY_CHECKS=1;"
mysql -uroot -p'kunhaohe-123' 48pocket < /tmp/schema.sql
