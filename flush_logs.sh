files=$(find /home/uwcc-admin/curw_sim_db_utils/ -name "*.log" -type f)

for file in $files;
do
  echo $file
  cat /dev/null > $file
done
