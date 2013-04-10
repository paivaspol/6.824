
for i in {1..10}
do
	go test >> test.log 2> error.log;
done;
