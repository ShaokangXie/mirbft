#! /bin/bash
# This compiles the protocol buffer files.
# Needs to be run every time any of the *.proto files changes.
# Always run from the project parent directory.

# protoc -I protobufs --go_out=plugins=grpc:protobufs protobufs/*.proto

protoc \
  -I protobufs \
  --go_out=protobufs --go_opt=paths=source_relative \
  --go-grpc_out=protobufs --go-grpc_opt=paths=source_relative \
  protobufs/*.proto