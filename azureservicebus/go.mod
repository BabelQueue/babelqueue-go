module github.com/babelqueue/babelqueue-go/azureservicebus

go 1.23.0

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.10.0
	github.com/babelqueue/babelqueue-go v1.0.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.18.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/go-amqp v1.4.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/text v0.27.0 // indirect
)

// In-repo development: resolve the core locally. Consumers ignore replace
// directives in dependencies and use the required version from the proxy.
replace github.com/babelqueue/babelqueue-go => ../
