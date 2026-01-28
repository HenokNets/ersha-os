# ersha-tls

This crate provides helpers to upgrade a standard `TcpStream` to a `TlsStream`
for use in **ersha-dispatch** and **ersha-prime**.

**This will:**

* Generate a local Root CA.
* Create and sign certificates for the Server (`prime`) and Client (`dispatch`).
* Automatically move the keys to their respective `keys/` directories in the
  neighbor crates.
* Clean up all intermediate temporary files.
