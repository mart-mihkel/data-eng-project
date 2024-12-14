#!/bin/bash

TARGET_DIR="/tmp/historical_weather"

mkdir --parents $TARGET_DIR

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Haapsalu-2007-juuni-2024.xlsx" -o "$TARGET_DIR/Haapsalu.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Haademeeste-2013-juuni-2024.xlsx" -o "$TARGET_DIR/Haademeeste.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Jogeva-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Jogeva.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Johvi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Johvi.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Kihnu-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Kihnu.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Kunda-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Kunda.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Kuusiku-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Kuusiku.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Laane-Nigula-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Laane-Nigula.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Narva-19.12.2013-juuni-2024.xlsx" -o "$TARGET_DIR/Narva.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Pakri-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Pakri.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Parnu-01.04.2019-juuni-2024.xlsx" -o "$TARGET_DIR/Parnu.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2023/02/Parnu-Sauga.xlsx" -o "$TARGET_DIR/Parnu-Sauga.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Ristna-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Ristna.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Roomassaare-2008-juuni-2024.xlsx" -o "$TARGET_DIR/Roomassaare.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Ruhnu-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Ruhnu.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Sorve-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Sorve.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tallinn-Harku-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Tallinn-Harku.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tartu-Toravere-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Tartu-Toravere.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tiirikoja-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Tiirikoja.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tooma-2009-juuni-2024.xlsx" -o "$TARGET_DIR/Tooma.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Turi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Turi.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Valga-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Valga.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Viljandi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Viljandi.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Vilsandi-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Vilsandi.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Virtsu-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Virtsu.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Voru-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Voru.xlsx"
sleep 1

curl "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Vaike-Maarja-2004-juuni-2024.xlsx" -o "$TARGET_DIR/Vaike-Maarja.xlsx"
sleep 1

