set date=%date:~10,4%-%date:~4,2%-%date:~7,2%
set date=2022-11-16
.\python\Scripts\python extractor.py --date "%date%" --source "c:\\0S-Tmini\Data\AllUsersData\Repositories\Default" --destination "c:\Orders"
PAUSE