# Mouse knockout data

```
# Get a copy of the postgres db dump
wget http://www.informatics.jax.org/downloads/database_backups/mgd.postgres.dump -O dump/mgd.postgres.dump

# Init services
docker-compose up --build -d

# restore db dump
docker-compose exec db bash
# this might take a while
pg_restore -j 8 --clean --if-exists --no-owner -U mouse -d mouse /dump/mgd.postgres.dump

# open up web ui at http://localhost:5433, with following configs
# user: mouse, pass: mouse, db: mouse, ssl: disable
```

```
# side note: winding down
docker-compose down --remove-orphans
docker volume rm mouse_knockout_mouse-data
```
