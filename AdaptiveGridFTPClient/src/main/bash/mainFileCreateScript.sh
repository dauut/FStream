SECONDS=0
. ./create_files.sh small-1 small 0 99 1 25
. ./create_files.sh small-2 small 100 149 1 50
. ./create_files.sh small-3 small 150 549 1 25
. ./create_files.sh small-4 small 550 679 1 75
. ./create_files.sh small-5 small 680 1679 1 25
. ./create_files.sh small-6 small 1680 2019 1 75
. ./create_files.sh small-7 small 2020 5019 1 15
. ./create_files.sh small-8 small 5020 5539 1 85
. ./create_files.sh small-9 small 5540 9539 1 20
. ./create_files.sh small-10 small 9540 10539 1 80
#. ./create_files.sh small-11 small 10540 15539 1 25
#. ./create_files.sh small-12 small 15540 17239 1 75

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
