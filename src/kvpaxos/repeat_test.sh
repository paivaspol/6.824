
for i in {1..10}
do
	echo "starting test $i"
	go test >> "test/test$i.log" 2> error.log;
	tail "test/test$i.log" -n 3
done;
