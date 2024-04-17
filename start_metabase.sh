# Pre-filled database: 
# 1. create directory metabase.db
# 2. copy metabase db files with dashboard settings to ./metabase.db
# (^^^^ included in repo, so you get it automatically)
#  --name=metabase --network=bridge
docker run -d -p 3000:3000 -v "./metabase.db:/metabase.db" metabase/metabase

# login: john@mailinator.com pass: Jj123456
# Dashboard on the main screen
# Enjoy! 