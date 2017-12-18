# CPSC 433 Mini Project: Chord #
## Tony Jiang and Ian Zhou ##

### Introduction ###

For our CPSC 433 mini project we implemented Chord, a peer-to-peer distributed 
hash table. In particular, we implemented a lookup protocol that allows users
to query any node about which node owns a particular key, and a distributed hash
table protocol for getting and putting key/value pairs, where the key and values
are longs.

### Files ###

We organized our source code in the following way:

- AbstractNode: Implements the Chord protocol, with node-to-node communication
                left unimplemented.
- Debug: Provides a class for debug output.
- Endpoint: Holds tuple of (host, port, key); this is how nodes are uniquely
            identified.
- LocalNode: Implementation of AbstractNode where multiple nodes exist in the
             same process. Used for easy unit testing.
- Node: Implementation of AbstractNode for node-to-node communication over
        sockets.
- NodeProgram: Contains the main class that users can run as an interface for
               creating nodes and inputting commands pertaining to those nodes.
- NodeTest: Contains unit tests for the nodes.
- Utils: Provides utility functions used by multiple source files, like hashing.

### How to Run ###

To run the program, run `java -cp chord.jar NodeProgram` and follow the
instructions from there.

To run tests, run `java -cp chord.jar NodeTest`. If the tests are successful,
you should get "Tests succeeded!"
