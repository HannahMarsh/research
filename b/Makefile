
default: build

build: export GO111MODULE=on
build:
	$(CGO_FLAGS) go build -o bin/go-ycsb bench/*

check:
	golint -set_exit_status db/... bench/... generator/...

