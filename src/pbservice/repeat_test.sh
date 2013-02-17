
for i in {1..50}
do
	go test >> repeat_test_output 2> error.log;
done;
