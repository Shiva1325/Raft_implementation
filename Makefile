CHAT_DIR = chat-system

gen:
	protoc --proto_path=proto proto/*.proto --go_out=. --go-grpc_out=require_unimplemented_servers=false:.

clean:
	del .\pb

server:
	go run cmd/server/server_main.go --port=12000
server1:
	go run cmd/server/server_main.go --id=1
server2:
	go run cmd/server/server_main.go --id=2
server3:
	go run cmd/server/server_main.go --id=3
server4:
	go run cmd/server/server_main.go --id=4
server5:
	go run cmd/server/server_main.go --id=5

client:
	go run main.go 
test:
	go test -cover  ./...