## Decentralized Traffic Management System (DTMS)

> The project aims to provide an efficient and novel approach to traffic management without centralized control in the future.

### Goal

Let decentralized autonomous vehicles reach consistent routing decisions at intersection under highly dynamic and insecure environment in real time.

### Problems
1. Autonomous vehicles need to broadcast the message reliably with unknown number of participants involved in coordination.
2. Autonomous vehicles need robust yet efficient routing strategies to satisfy strict safety requirements with tight timing constraints.
3. Autonomous vehicles need to query **correct** available right of way information at intersection.

### Solutions
1. Tailored the reliable broadcast algorithm described in the paper [Byzantine Agreement with Unknown Participants and Failures](https://arxiv.org/abs/2102.10442), which relied heavily upon **synchronous** network, to a more realistic scenario.
2. Adopted the concept of [conflict-free replicated data type](https://arxiv.org/abs/1805.06358) to devise a coordination strategy to avoid redundant rollback and retry mechanism when resolving conflicting routing requests.
3. Devised a relaxed form of distributed queue (derived from [Ricart–Agrawala distributed mutex algorithm](https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm)) for multi-agent task allocation by accepting eventual consistency to maximize the throughput of enqueue operations while minimize the latency of dequeue operations.
4. Leveraged [blockchain](https://en.wikipedia.org/wiki/Blockchain) data structure to record routing decisions made in each round **securely**, and built [R-tree](https://en.wikipedia.org/wiki/R-tree) for storing object locations and spatial querying.

#### TODO
- [X] Reliable broadcast
- [X] Distributed lock
- [X] Eventual queue
- [X] Conflict-free replicated data type
    - [X] [Bounded counter](https://pages.lip6.fr/syncfree/attachments/article/59/boundedCounter-white-paper.pdf)
    - [X] [PN counter](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#PN-Counter_(Positive-Negative_Counter))
    - [ ] Customized routing decision data type
- [ ] Road-site unit
    - [X] R-tree
    - [ ] Blockchain
        - [X] Init block, Decision block, Route block, Phantom block
        - [X] Decision chain
        - [ ] Route chain
