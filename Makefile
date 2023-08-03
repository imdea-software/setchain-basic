all: rbbc zeromq

dso:
	CGO_ENABLED=1 go build -o build/dso cmd/dso/main.go

dpo2:
	CGO_ENABLED=1 go build -o build/dpo2 cmd/dpo2/main.go

rbbc:
	CGO_ENABLED=1 go build -o build/rbbc cmd/rbbc/main.go

zeromq:
	CGO_ENABLED=1 go build -o build/zeromq cmd/zeromq/main.go

test:
	go test -cover imdea.org/redbelly/p2p/zeromq
	go test -cover imdea.org/redbelly/rbbc
	

clean:
	rm -rf build

rebuild: clean all

.PHONY: rbbc test zeromq clean rebuild dso dpo2
