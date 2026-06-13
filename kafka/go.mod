module github.com/babelqueue/babelqueue-go/kafka

go 1.23.0

require (
	github.com/babelqueue/babelqueue-go v1.0.0
	github.com/segmentio/kafka-go v0.4.47
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

replace github.com/babelqueue/babelqueue-go => ../
