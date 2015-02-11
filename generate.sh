#/bin/bash
# Generates new go files based on the proto files

# install required tools
go get -u github.com/golang/protobuf/{proto,protoc-gen-go}

# check if go bin is in path
if [[ $PATH != *"$GOPATH/bin"* ]]
then
    echo "\$GOPATH/bin not in PATH!"
    exit 1
fi

protoc --go_out=request/. request/*.proto
protoc --go_out=response/. response/*.proto