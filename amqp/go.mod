module github.com/babelqueue/babelqueue-go/amqp

go 1.21

require (
	github.com/babelqueue/babelqueue-go v0.2.0
	github.com/rabbitmq/amqp091-go v1.10.0
)

// In-repo development: resolve the core locally. Consumers ignore replace
// directives in dependencies and use the required version from the proxy.
replace github.com/babelqueue/babelqueue-go => ../
