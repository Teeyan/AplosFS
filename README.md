# JichuFS
simple, fault tolerant, distributed file system

## Jichu
Why is this a simple file system? Because it runs from the command line and is currently only capable of
uploading/downloading/querying files. Also the primary Introducer node for our network has a rough time recovering from failures.
It is fault tolerant up to 3 consecutive failures though and is "surprisingly" performant.

## Fault Tolerance
Each file has 3 replicas stored at different servers. On failure, we ensure that all files on the failed server will be 
re-replicated elsewhere. Failure detection is based on the SWIM protocol outlined [here](http://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)

Election for a coordinator server is done through a Ring-Based Leader Election protocol.

## TODO
- Add Support For LARGE file uploads
- Build a Frontend for Submitting Requests
- Add More Supported File Ops
- Clean Up Redundant Code / Add More Documentation
- Add More Logging
- Switch to Bully Algorithm Leader Election
- Dynamically Control Introducer (to allow for Introducer failure)
