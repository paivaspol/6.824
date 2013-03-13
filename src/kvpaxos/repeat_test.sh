
for i in {1..5}
do
	go test >> test.log 2> error.log;
done;
