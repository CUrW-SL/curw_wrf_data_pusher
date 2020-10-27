files=$(find /home/uwcc-admin/ -name "*.log" -type f)

for file in $files;
do
  echo $file
  cat /dev/null > $file
done
