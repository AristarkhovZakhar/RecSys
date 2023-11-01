docker compose restart
docker exec -it parser-parser-1 /bin/bash
pip install -r /opt/src/backend/parser/requirements.txt
nohup python3 /opt/src/backend/parser/telegram_parser.py
exit
