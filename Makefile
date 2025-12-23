.PHONY: proto run-scheduler run-worker run-agent

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	--go-grpc_out=. --go-grpc_opt=paths=source_relative \
	proto/astra.proto

up:
	docker-compose up -d

down:
	docker-compose down

run-scheduler:
	go run cmd/scheduler/main.go

run-worker:
	go run cmd/worker/main.go

run-agent:
	cd reasoning && source venv/bin/activate && python agent.py