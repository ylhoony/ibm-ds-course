import subprocess, os

# bash_command='grep -oP "^\\d{1,3}(\\.\\d{1,3}){3}" accesslog.txt > extracted_data.txt'
bash_command='cut -d " " -f1 ./accesslog.txt > ./extracted_data.txt'

os.system(bash_command)