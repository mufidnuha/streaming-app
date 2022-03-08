from itertools import count
from google_play_scraper import app

app = [
    {
        "app_name":"wetv",
        "gp_id":"com.tencent.qqlivei18n",
        "apple_id":"1441531611"
    },
    {
        "app_name":"netflix",
        "gp_id":"com.netflix.mediaclient",
        "apple_id":"363590051"
    },
    {
        "app_name":"viu",
        "gp_id":"com.vuclip.viu",
        "apple_id":"1044543328"
    },
    {
        "app_name":"vidio",
        "gp_id":"com.vidio.android",
        "apple_id":"1048858798"
    },
    {
        "app_name":"disney_hotstar",
        "gp_id":"in.startv.hotstar.dplus",
        "apple_id":"1524156685"
    },
]
language='id'
country='id'

result = app(
    app_id=app[0]['app_id'],
    lang=language, # defaults to 'en'
    country=country # defaults to 'us'
)

