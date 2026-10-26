module github.com/babelqueue/babelqueue-go/azureservicebus

go 1.25.0

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.11.0
	github.com/babelqueue/babelqueue-go v1.18.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.23.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/Azure/go-amqp v1.7.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

// In-repo development: resolve the core locally. Consumers ignore replace
// directives in dependencies and use the required version from the proxy.
replace github.com/babelqueue/babelqueue-go => ../
