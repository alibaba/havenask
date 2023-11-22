---
toc: content
order: 1
---

# Performance
## General retrieval Performance Testing
| Small Data Scenario | Big Data Scenario
-- | -- | --
Test Scenario | Dataset: Public Dataset (esrally Official Test Set) Document Number: 570000 (22G) | Dataset: Commodity Order Dataset Document Number: 0.1 billion (395G)
Test method | 90000 query without repeated words | 260000 query without repeated words
Test environment | OpenSearch Recall Engine Edition 16-core 64 GB machine | OpenSearch Recall Engine Edition 16-core 64 GB machine
Duration | 90 Percentile: 7.33ms <br>99 Percentile: 32.72ms <br>avg 4.51ms | 90 Percentile: 3.73ms <br>99 Percentile: 11.71ms <br>avg 3.07ms
QPS | 6490 | 8680
Index Storage | 16G | 96G
Index Compression Ratio | 72.7% | 24.3%
CPU load | 75% | 78%
Memory usage | 13.4G | 22G

</div>

## vector retrieval Performance Testing

<div class="lake-content" typography="classic">

Data Scenarios | Small Data | Medium Data | Big Data
-- | -- | -- | --
Data situation | sift public dataset 1 million data 128 dimension | deep public dataset 10 million data 96 dimension | alibaba internal dataset 0.1 billion data 384 dimension
Test machine specifications | 16 vCPUs, 64 GiB <br>local disk storage: 1788 GiB <br>ecs.i2gne.4xlarge | 16 vCPUs, 64 GiB <br>local disk storage: 1788 GiB <br>ecs.i2gne.4xlarge | 64 vCPUs, 512 GiB <br>local disk storage: 3576 GiB <br>ecs.i4r.16xlarge
Raw data size | 501MB | 3.6GB | 288GB
Vector algorithm | Qc(int8 quantization) | HNSW | Qc(int8 quantization) | HNSW | Qc(int8 quantization)
Index creation time | 14s | 53s | 20.6min | 48.43min | 6.29h
CPU Load Situation | Low Load | High Load | Low Load | High Load | Low Load | High Load | Low Load | High Load | Low Load | High Load
CPU utilization (%) | 6.3 | 81.9 | 6.4 | 83.9 | 6.5 | 87.2 | 6.6 | 84.3 | 6.2 | 87.2
Memory usage (GB) | 0.647 | 0.647 | 1.08 | 1.08 | 1.7 | 1.7 | 6.2 | 6.2 | 40.4 | 40.4
QPS | 460.37 | 5013.07 | 383.62 | 4070.47 | 147.54 | 1638.78 | 174.57 | 1605.92 | 28.61 | 308.03
Duration (ms) | 2.17 | 9.61 | 2.6 | 11.8 | 6.77 | 19.53 | 5.72 | 21.29 | 139.48 | 177.9
Recall Rate (%) | Top 1:99 <br>Top 10:97.1 <br>Top 50:95.82 <br>Top 100:94.94 | Top 1:100 <br>Top 10:98.8 <br>Top 50:99.1 <br>Top 100:97.9 | Top 1:100 <br>Top 10:98.09 <br>Top 50:96.99 <br>Top 100:96.86 | Top 1:100 <br>Top 10:100 <br>Top 50:99.72 <br>Top 100:99.54 | Top 1:96 <br>Top 10:97.1 <br>Top 50:96.78 <br>Top 100:96.67

</div>