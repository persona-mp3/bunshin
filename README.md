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
Build and run the binaries. To directly run a single instance run the bunshin binary
```bash
cargo build
cp ./target/debug/infra .
```

If you want to specify peers directly to bunshin
This will run three nodes on the provided ports
```bash
./infra --nodes 3001,3002,3003 
```

To stop each one, run the following commands
```bash
./infra stop 0 
./infra stop 1
./infra stop 2
```

To kill all at once
```bash
./infra stop-all
```

If unsure, there's an `infra-config.json` that will be created in the current 
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
}⏎                                
```
