#!/bin/bash
python purge.py > tmp.txt
rm tmp.txt

python twitch.py createchannel OverwatchLeague 137512364 > tmp.txt
sed -i "s/u'/'/g" tmp.txt
diff tmp.txt ./validation_output/create_owl.txt
rm tmp.txt

python twitch.py createchannel RiotGames 36029255 > tmp.txt
sed -i "s/u'/'/g" tmp.txt
diff tmp.txt ./validation_output/create_riot.txt
rm tmp.txt

python twitch.py parsetopspam shdvsnyxl.json > tmp.txt
diff tmp.txt ./validation_output/pts_owl.txt
rm tmp.txt

python twitch.py parsetopspam league.json > tmp.txt
diff tmp.txt ./validation_output/pts_league.txt
rm tmp.txt

python twitch.py gettopspam 137512364 451603129 > tmp.txt
diff tmp.txt ./validation_output/gts_owl.txt
rm tmp.txt

python twitch.py gettopspam 36029255 497295395 > tmp.txt
diff tmp.txt ./validation_output/gts_league.txt
rm tmp.txt

python twitch.py storechatlog shdvsnyxl.json > tmp.txt
diff tmp.txt ./validation_output/scl_owl.txt
rm tmp.txt

python twitch.py storechatlog league.json > tmp.txt
diff tmp.txt ./validation_output/scl_league.txt
rm tmp.txt

python twitch.py querychatlog "stream_id eq 451603129" "user eq Moobot" > tmp.txt
diff tmp.txt ./validation_output/query_moobot_owl.txt
rm tmp.txt

python twitch.py querychatlog "stream_id eq 497295395" "user eq Moobot" > tmp.txt
diff tmp.txt ./validation_output/query_moobot_league.txt
rm tmp.txt

python twitch.py querychatlog "stream_id eq 451603129" "offset gteq 10" "offset lteq 100" > tmp.txt
diff tmp.txt ./validation_output/query_offset_owl.txt
rm tmp.txt

python twitch.py querychatlog "stream_id eq 497295395" "offset gteq 10" "offset lteq 100" > tmp.txt
diff tmp.txt ./validation_output/query_offset_league.txt
rm tmp.txt
