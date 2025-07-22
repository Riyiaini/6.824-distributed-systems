
run-coordinator:
	@echo "Running coordinator.. "
	@go run mrcoordinator.go pg-*.txt

run-worker: wc.so
	@echo "Running worker.. "
	@go run mrworker.go wc.so
	
times ?= 10

multiple-run: wc.so
	@for i in $$(seq 1 $(times)); do \
		go run mrworker.go wc.so & \
	done

%.so: ../mrapps/%.go
	@echo "Building plugin.. "
	@go build -buildmode=plugin -o $@ $<

clean:
	rm -f *.so
	rm -rf ../temp
	rm -f mr-out-*

.PHONY: clean run-worker run-coordinator multiple-run