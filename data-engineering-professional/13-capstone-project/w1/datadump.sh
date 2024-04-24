#! /usr/bin/bash

mysqldump --host=127.0.0.1 --user=root --password=MTA5NzMteWxob29u --flush-logs --delete-master-logs  --databases sales --tables sales_data > sales_data_backup.sql
