openpilot Log Uploader
---
This script allows you to upload openpilot logs (rlogs/camera footages) in PC environment.

tested in ubuntu 16.04 + python 3.5


INSTALLATION
---
1. modify ```config.py``` for dongle_id (fetch from EON ```/data/params/d/DongleId```)
2. download id_rsa file from EON ```/persist/comma/id_rsa``` to ```./id_rsa```
3. download logs from EON ```/sdcard/realdata/``` to ```./realdata/```
4. run ```python3 uploader.py```