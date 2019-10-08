SECONDS=0
. ./create_files.sh large-1 large 0 199 1 150
. ./create_files.sh large-2 large 200 249 1 600
. ./create_files.sh large-3 large 250 749 1 200
. ./create_files.sh large-4 large 750 759 1 1000
. ./create_files.sh large-5 large 760 859 1 250
. ./create_files.sh large-6 large 860 884 1 1000
. ./create_files.sh large-7 large 885 1304 1 200
. ./create_files.sh large-8 large 1305 1359 1 800
. ./create_files.sh large-9 large 1360 1759 1 200
. ./create_files.sh large-10 large 1760 1859 1 800
#. ./create_files.sh large-11 large 10540 15539 1 25
#. ./create_files.sh large-12 large 15540 17239 1 75

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
