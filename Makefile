build: dep
	CGO_ENABLED=0 GOOS=linux go build ./...

dep:
	@echo ">> Downloading Dependencies"
	@go mod download

test-all: test-unit test-integration

test-all-no-infra: test-unit test-integration-no-infra

test-unit: dep
	@echo ">> Running Unit Test"
	@go test -tags=unit -count=1 -cover -covermode=atomic -v ./...

test-integration-no-infra: dep
	@echo ">> Running Integration Test"
	@env $$(cat .env.testing | xargs) go test -tags=integration -failfast -count=1 -p=1 -cover -covermode=atomic -v ./...

test-integration: test-infra-up test-integration-no-infra test-infra-down

test-infra-up: test-infra-down
	@echo ">> Starting Rabbit MQ"
	@docker run --name go-msgbuzz-test-rabbitmq -p 56723:5672 -p 15672:15672 -d --rm rabbitmq:3-management
	@docker exec go-msgbuzz-test-rabbitmq sh -c 'sleep 5; rabbitmqctl wait /var/lib/rabbitmq/mnesia/rabbit@$$(hostname).pid'

test-infra-down:
	@echo ">> Shutting Down Rabbit MQ"
	@-docker kill go-msgbuzz-test-rabbitmq

.PHONY: build dep test-all test-all-no-infra test-unit test-integration test-integration-no-infra test-infra-up test-infra-down