# Scalable Surfstore

## Extend your surfstore project
1. Make a copy of your solution if you want to:
```shell
mkdir proj4
cp -r proj3/* proj4
cd proj4
```

2. copy over consistent hashing files and new new .proto service defination
```shell
mkdir cmd/SurfstorePrintBlockMapping
cp /yourPath/starter-code/cmd/SurfstorePrintBlockMapping/main.go cmd/SurfstorePrintBlockMapping/
cp /yourPath/starter-code/pkg/surfstore/ConsistentHashRing.go pkg/surfstore
cp /yourPath/starter-code/pkg/surfstore/SurfStore.proto pkg/surfstore
cp /yourPath/starter-code/pkg/surfstore/SurfstoreInterfaces.go pkg/surfstore
```

4. manually change MetaStore.go
```diff
type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
-	BlockStoreAddr string
+	BlockStoreAddrs    []string
+	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

-func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
-	panic("todo")
-}
+func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
+	panic("todo")
+}
+
+func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
+	panic("todo")
+}
...

-func NewMetaStore(blockStoreAddr string) *MetaStore {
+func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
-		BlockStoreAddr: blockStoreAddr,
+		BlockStoreAddrs:    blockStoreAddrs,
+		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
```

5. add the following function in BlockStore.go
```diff
+// Return a list containing all blockHashes on this block server
+func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
+	panic("todo")
+}
```

6. extend SurfstoreRPCClient.go 
```diff
+func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
+	panic("todo")
+}
...

-func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
-	panic("todo")
-}
+func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
+	panic("todo")
+}
+
+func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
+	panic("todo")
+}
```

7. change your code in `cmd/SurfstoreServerExec/main.go` to handle multiple tail aruguments. In previous project, we only have one tail argument indicating the single block server's address. Now we want to handle multiple arguments to configure multiple block servers. 



## Protocol buffers

<!-- ```diff
service BlockStore {
    rpc GetBlock (BlockHash) returns (Block) {}
    rpc PutBlock (Block) returns (Success) {}
    rpc HasBlocks (BlockHashes) returns (BlockHashes) {}
+   rpc GetBlockHashes (google.protobuf.Empty) returns (BlockHashes) {}
}

service MetaStore {
    rpc GetFileInfoMap(google.protobuf.Empty) returns (FileInfoMap) {}
    rpc UpdateFile(FileMetaData) returns (Version) {}
-   rpc GetBlockStoreAddr(google.protobuf.Empty) returns (BlockStoreAddr) {}
+   rpc GetBlockStoreMap(BlockHashes) returns (BlockStoreMap) {}
+   rpc GetBlockStoreAddrs(google.protobuf.Empty) returns (BlockStoreAddrs) {}
}
``` -->

The gRPC service in `SurfStore.proto` haven been changed. **You need to regenerate the gRPC client and server interfaces from our .proto service definition.** We do this using the protocol buffer compiler protoc with a special gRPC Go plugin (The [gRPC official documentation](https://grpc.io/docs/languages/go/basics/) introduces how to install the protocol compiler plugins for Go).

```shell
protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/surfstore/SurfStore.proto
```

Running this command generates `SurfStore.pb.go` and `SurfStore_grpc.pb.go` in the `pkg/surfstore` directory.



## Usage
1. Run your server using this:
```shell
go run cmd/SurfstoreServerExec/main.go -s <service> -p <port> -l -d (BlockStoreAddr*)
```
Here, `service` should be one of three values: meta, block, or both. This is used to specify the service provided by the server. `port` defines the port number that the server listens to (default=8080). `-l` configures the server to only listen on localhost. `-d` configures the server to output log statements. Lastly, (BlockStoreAddr\*) are the BlockStore addresses that the server is configured with. 

2. Run your client using this:
```shell
go run cmd/SurfstoreClientExec/main.go -d <meta_addr:port> <base_dir> <block_size>
```

3. Print block mapping using this:
```shell
go run cmd/SurfstorePrintBlockMapping/main.go -d <meta_addr:port> <base_dir> <block_size>
```

## Examples:

1.
```shell
Run the commands below on separate terminals (or nodes)
> go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l
> go run cmd/SurfstoreServerExec/main.go -s block -p 8082 -l
> go run cmd/SurfstoreServerExec/main.go -s meta -l localhost:8081 localhost:8082
```
The first two lines start two servers that services BlockStore interface and listens to localhost on port 8081 and 8082. The third line starts a server that services MetaStore interface, listens to localhost on port 8080, and references the BlockStore we created as the underlying BlockStore. (Note: if these are on separate nodes, then you should use the public ip address and remove `-l`)

2. From a new terminal (or a new node), run the client using the script provided in the starter code (if using a new node, build using step 1 first). Use a base directory with some files in it.
```shell
> mkdir dataA
> cp ~/pic.jpg dataA/ 
> go run cmd/SurfstoreClientExec/main.go localhost:8080 dataA 4096
```
This would sync pic.jpg to the server hosted on localhost:8080, using `dataA` as the base directory, with a block size of 4096 bytes.

3. From another terminal (or a new node), run PrintBlockMapping to check which blocks a block server has. 
```shell
> go run cmd/SurfstorePrintBlockMapping/main.go localhost:8080 dataB 4096
```
The output willl be a map from block hashes to server names. 

## Testing 
We will conduct similar tests to the previous project, but this time on multiple servers. Make sure your surfstore supports multiple servers, and ClientSync works as expected. In addition, we will check your block mapping by calling SurfstorePrintBlockMapping. 

On gradescope, only a subset of test cases will be visible, so we highly encourage you to come up with different scenarios like the one described above. You can then match the outcome of your implementation to the expected output based on the theory provided in the writeup.
