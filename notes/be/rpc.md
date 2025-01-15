doris 使用 thrift 和 grpc 两种通信框架，其协议文件位于：  
| 框架 | 协议位置 |位置 |
|------|---------|-----|
| grpc | gensrc/proto |FE 和 BE 通信。BE 是服务端，以 brpc 启动，端口是 brpc_port；FE 是客户端。主要接口都在 gensrc/proto/internal_service.proto|
| thrift | gensrc/thrift |FE 和 BE 通信。BE 作为服务端，以 thrift server 启动，端口是 be_port; FE 是客户端。主要接口都在 gensrc/thrift/BackendService.thrift |
