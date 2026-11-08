Mock testing proof of concept

```rust
#[derive(Default)]
pub struct MockDialer {
    responses: HashMap<String, Rpc>,
    down: HashSet<String>,
    calls: Mutex<Vec<(String, Rpc)>>,
}

impl MockDialer {
    pub fn respond(mut self, addr: &str, resp: Rpc) -> Self {
        self.responses.insert(addr.to_string(), resp);
        self
    }

    pub fn down(mut self, addr: &str) -> Self {
        self.down.insert(addr.to_string());
        self
    }

    pub fn calls(&self) -> Vec<(String, Rpc)> {
        self.calls.lock().unwrap().clone()
    }
}

impl MakeRpc for MockDialer {
    fn call(&self, addr: &str, req: Rpc) -> Result<Rpc> {
        self.calls.lock().unwrap().push((addr.to_string(), req));

        if self.down.contains(addr) {
            return Err(anyhow!("{addr} is down"));
        }
        self.responses
            .get(addr)
            .cloned()
            .ok_or_else(|| anyhow!("no response configured for {addr}"))
    }
}
```
