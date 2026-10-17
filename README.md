## bushin
bushin is a raft implementation to make a storage engine, jkvs distributed.

## Prerequsites
Rust toolchain installed

## Run application
### 1. Clone repository
```bash
git clone https://github.com/persona-mp3/bunshin.git 
cd bunshin
```

### 2. Run bunshin instance
Build and run the binaries. To directly run a single instance run the bunshin binary
```bash
cargo build
cp ./target/debug/bunshin .
```

```bash
./bunshin --node-id <any_number> <port_to_listen> 
```

If you want to specify peers directly to bunshin
```bash
./bunshin --node-id <any_number> --port <port_to_listen>  --peers <more_port_numbers>
```


### 3. To  run a cluster
Build and run the binaries. To directly create a cluster, run the infra binary
```bash
cargo build
cp ./target/debug/infra .
```

Specify the ports each will listen on
```bash
./infra --nodes 3001,3002,3003 
```

To stop the whole cluster
```bash
./infra stop-all
```

To target a specific one, use the 0 based index If unsure, there's an `infra-config.json` that will be created in the current 
```bash
./infra stop 0 
./infra stop 1
./infra stop 2
```

directory and it would look something like this
```json
{
  "0": {
    "idx": 0,
    "process_id": 1479100,
    "listen_addr": "3003"
  },
  "2": {
    "idx": 2,
    "process_id": 1479102,
    "listen_addr": "3002"
  },
  "1": {
    "idx": 1,
    "process_id": 1479101,
    "listen_addr": "3001"
  }
}                                
```

Note: You can also skip the auto copy simply by running the build.sh file 

## Tests
End to end tests are coded inside the `tester` crate. To test, you need to first 
run a node, and provide the same arguments to tester. After running a build 
the  binary `tester` is created. 

```bash
./tester --node-id <id_of_node_running> --port <port_node_is_listening>
```
