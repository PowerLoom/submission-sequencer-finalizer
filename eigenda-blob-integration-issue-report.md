**Is your feature request related to a problem?**
The primary purpose of this component is to ensure reliable data availability for snapshots of smart contract states at specific block heights. The current implementation lacks the core functionality to submit these data blobs to the EigenDA network, which means it cannot fulfill this critical purpose.

**Describe the solution you'd like**
Implement the functionality to upload JSON data blobs to the EigenDA V2 disperser. These blobs contain detailed snapshots of smart contract state values at specific block heights. This will involve completing the `StoreOnEigenDA` function in `pkgs/eigenda/eigenda.go` to handle the serialization and submission of these batches using the configured EigenDA V2 client.

**Describe alternatives you've considered**
The primary alternative is to use IPFS for data availability, for which a partial implementation already exists. However, this does not align with the project's goal of leveraging EigenDA for its specific data availability and security guarantees.

**Additional context**
The implementation must use the V2 methods from the official `github.com/Layr-Labs/eigenda/api/clients/v2` library. It is also critical to develop a corresponding test suite (`pkgs/eigenda/eigenda_test.go`) to ensure the reliability of the blob submission process, including proper handling of API responses and errors.