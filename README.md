Pipelines
=========

Just a toy PoC to experiment goroutines and channel patterns to create
generator functions chained into multiple processing steps pipelines. Ideally
any problem can be modeled as a pipeline, but the use case is particularly
powerful when dealing with streams of data, like aggregation of events from
monitored pages.

In this case a sample of visit events is used to provide a batch of data which
could very well be a real scenario in the case of an edge-computing
architecture gathering events by using agents on different regions, sending them
to any middleware to be aggregated and analyzed.

**Roadmap**

- Tumbling window and tolerance for late out of order events
- Dead-letter queue
- No roadmap, just play
