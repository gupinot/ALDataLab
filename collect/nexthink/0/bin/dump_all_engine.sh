Dir=$(dirname $0)
lst_engine=$Dir/$1

DATE=$(date +"%Y%m%d")
HEUR=$(date +"%H%M%S")

if [[ ! -f $lst_engine ]]
then
	echo "$DATE $HEURE ERROR in dump_all_engine.sh: file $lst_engine does not exist"
	exit 1
fi

for file in $(cat $lst_engine)
do
	sh $Dir/scp_engine.sh $file&
done
