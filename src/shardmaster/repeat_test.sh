
for i in {1..200}
do
	echo "starting test $i"
	go test >> "results/trial$i.log" 2> error.log;
	tail "results/trial$i.log" -n 3
done;