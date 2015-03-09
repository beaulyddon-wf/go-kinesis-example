Go - Amazon Kinesis Example
========

Uses Send Grid's [Go Kinesis lib](https://github.com/sendgridlabs/go-kinesis)

Extends their example.

Commands
--------

Prefix all commands with your `env` variables.

* AWS_ACCESS_KEY
* AWS_SECRET_KEY
* AWS_REGION_NAME

`
export AWS_ACCESS_KEY=YOUR_ACCESS_KEY; export AWS_SECRET_KEY=YOUR_SECRET_KEY; export AWS_REGION_NAME=region; go COMMAND
`

Setup
-----

Create a stream (you can also do this via [Amazon's console](https://console.aws.amazon.com/kinesis/home))

`
 go run producer/main.go -stream=test1
`

Run
---

### Consumer

`
go run consumer/main.go -stream=test1
`

### Producer

`
go run producer/main.go -stream=test1 -batch_size=1 -batch_count=1
`


Tear Down
---------

Delete a stream (you can also do this via [Amazon's console](https://console.aws.amazon.com/kinesis/home))

`
go run producer/main.go -setup=false -stream=test1
`
