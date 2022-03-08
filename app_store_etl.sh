apps=( wetv netflix viu vidio disney_hotstar )
gp_id=( 'com.tencent.qqlivei18n' 'com.netflix.mediaclient' 'com.vuclip.viu' 'com.vidio.android' 'in.startv.hotstar.dplus')
app_id=( '1441531611' '363590051' '1044543328' '1048858798' '1524156685' )
country=id
j=4
python3 ingest_apple_store.py \
    --app_id="${app_id[$j]}" \
    --app_name="${apps[$j]}" \
    --country="$country"