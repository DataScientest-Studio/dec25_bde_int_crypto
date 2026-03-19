#!/bin/sh
set -eu

# Install the hourly collector schedule into the container's cron directory.
cp /app/scripts/collector.crontab /etc/cron.d/binance-collector
chmod 0644 /etc/cron.d/binance-collector

# Run one sync immediately so the container does not wait an hour before the
# first historical backfill/update.
cd /app
/usr/local/bin/python -m src.service.batch.binance_historical_collector \
  >> /proc/1/fd/1 2>> /proc/1/fd/2

# Keep cron in the foreground so the container stays up and the hourly job runs.
exec cron -f
