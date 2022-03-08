apps=( wetv netflix viu vidio disney_hotstar )
gp_id=( 'com.tencent.qqlivei18n' 'com.netflix.mediaclient' 'com.vuclip.viu' 'com.vidio.android' 'in.startv.hotstar.dplus')
country=id

length=${#apps[@]}
for (( j=0; j<length; j++ ));
do
    python3 ingest_google_play.py \
        --app_id="${gp_id[$j]}" \
        --app_name="${apps[$j]}" \
        --language='id' \
        --country="$country"

    python3 ingest_google_play.py \
        --app_id="${gp_id[$j]}" \
        --app_name="${apps[$j]}" \
        --language='en' \
        --country="$country"
done