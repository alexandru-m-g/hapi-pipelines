#!/bin/sh

today=$(date +%Y%m%d-%H%M%S)

echo "Started at $today"

for table in location admin1 admin2 population operational_presence humanitarian_needs
do
  echo "Saving $table"
  psql -U postgres -d hapi -c "\copy (select * from ${table}_view) TO STDOUT (FORMAT csv, DELIMITER ',',  HEADER);" | tee >(gzip > shared/${table}.csv.gz) >(md5sum > shared/${table}.hash) >/dev/null
done

# end
end=$(date +%Y%m%d-%H%M%S)

echo "Ended at $end."
