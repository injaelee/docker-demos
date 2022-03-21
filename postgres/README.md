# Execution

## Bring Up
```
docker-compose up
```

## Bring Down
```
-- if not running on daemon mode
-- doing a CTRL-C must follow a 'down'
--
docker-compose down

-- delete volume if necessary
-- if you want to start from scratch in the next 'up'
--
docker volume rm pg_clio_db_data
```
