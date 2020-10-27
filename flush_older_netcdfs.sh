cd /mnt/disks/wrf_nfs
files=$(find . -type f -name "*.nc")

for file in $files;
do
    echo "$file"
    FILE_MODIFIED_TIME=$(date -r ${file} +%s)
    CURRENT=$(date +%s)

    DIFF=$(((CURRENT-FILE_MODIFIED_TIME)/60/60/24))
    echo $DIFF

    if [ $DIFF -gt 90 ]
    then
      echo "Deleting..."
      echo $file
#      rm -v $file
    fi
done

