
for i in {1..100}
do
	go test >> test.log 2> error.log;
done;
