#!/bin/bash

TARGET_DIR="/tmp/historical_weather"

mkdir --parents $TARGET_DIR

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Dirhami-2011-juuni-2024.xlsx" -o "$TARGET_DIR/Dirhami-2011-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Haapsalu-2007-juuni-2024.xlsx" -o "$TARGET_DIR/Haapsalu-2007-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Haapsalu-sadam-2011-juuni-2024.xlsx" -o "$TARGET_DIR/Haapsalu-sadam-2011-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Heltermaa-2008-juuni-2024.xlsx" -o "$TARGET_DIR/Heltermaa-2008-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Haademeeste-2013-juuni-2024.xlsx" -o "$TARGET_DIR/Haademeeste-2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Jogeva-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Jogeva-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Johvi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Johvi-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Kihnu-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Kihnu-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Kunda-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Kunda-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Kuusiku-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Kuusiku-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Korgessaare-2015-juuni-2024.xlsx" -o "$TARGET_DIR/Korgessaare-2015-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Loksa-2011-juuni-2024.xlsx" -o "$TARGET_DIR/Loksa-2011-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Laane-Nigula-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Laane-Nigula-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Mustvee-2007-juuni-2024.xlsx" -o "$TARGET_DIR/Mustvee-2007-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Montu-2013-juuni-2024.xlsx" -o "$TARGET_DIR/Montu-2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Naissaare-01.06.2013-juuni-2024.xlsx" -o "$TARGET_DIR/Naissaare-01.06.2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Narva-19.12.2013-juuni-2024.xlsx" -o "$TARGET_DIR/Narva-19.12.2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2023/02/Narva-Joesuu.xlsx" -o "$TARGET_DIR/Narva-Joesuu.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Osmussaare-2013-juuni-2024.xlsx" -o "$TARGET_DIR/Osmussaare-2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Pakri-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Pakri-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Paldiski-2013-juuni-2024.xlsx" -o "$TARGET_DIR/Paldiski-2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Pirita-2011-juuni-2024.xlsx" -o "$TARGET_DIR/Pirita-2011-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Parnu-01.04.2019-juuni-2024.xlsx" -o "$TARGET_DIR/Parnu-01.04.2019-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2023/02/Parnu-Sauga.xlsx" -o "$TARGET_DIR/Parnu-Sauga.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Ristna-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Ristna-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Rohuneeme-2011-juuni-2024.xlsx" -o "$TARGET_DIR/Rohuneeme-2011-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Roomassaare-2008-juuni-2024.xlsx" -o "$TARGET_DIR/Roomassaare-2008-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Ruhnu-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Ruhnu-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Sorve-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Sorve-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tallinn-Harku-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Tallinn-Harku-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tartu-Toravere-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Tartu-Toravere-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tiirikoja-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Tiirikoja-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tooma-2009-juuni-2024.xlsx" -o "$TARGET_DIR/Tooma-2009-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Turi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Turi-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Vaindloo-2013-juuni-2024.xlsx" -o "$TARGET_DIR/Vaindloo-2013-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Valga-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Valga-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Viljandi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Viljandi-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Vilsandi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Vilsandi-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Virtsu-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Virtsu-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Voru-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Voru-2004-juuni-2024.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Vaike-Maarja-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Vaike-Maarja-2004-juuni-2024.xlsx"
sleep 1

