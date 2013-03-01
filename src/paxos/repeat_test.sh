
for i in {1..200}
do
	go test >> test.log 2> error.log;
done;
