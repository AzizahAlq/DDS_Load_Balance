# DDS-Based Dynamic Real-Time Load Balancing

This repository contains the implementation and experimental materials associated with our published paper:

## Publication

**A Distributed System Design Based on DDS Middleware for Dynamic Real-Time Load Balancing**

Azizah AlQahtani, Nadeen AlAmoudi, Manal AlShmrani, and Tarek Helmy

*Concurrency and Computation: Practice and Experience*, Wiley  
First published: 22 September 2026

DOI: https://doi.org/10.1002/cpe.70955

Paper: https://onlinelibrary.wiley.com/doi/10.1002/cpe.70955

## Overview

This work presents a distributed system design based on Data Distribution Service (DDS) middleware for dynamic real-time load balancing.

The system uses DDS publish–subscribe communication to exchange real-time resource information between distributed nodes and controllers. The design supports heterogeneous environments and includes a fault-tolerant dual-controller architecture to maintain system operation in the event of controller failure.

The system considers multiple node resource metrics, including:

- CPU utilization
- Available memory
- Battery level
- System load
- Node availability

Based on the latest resource information, the controller evaluates the available nodes and dynamically selects a suitable node for task execution.

## Main Features

- DDS-based publish–subscribe communication
- Dynamic real-time load balancing
- Heterogeneous distributed nodes
- Resource-aware node selection
- Real-time node monitoring
- Fault-tolerant dual-controller architecture
- Heartbeat-based controller failover
- DDS and Zenoh experimental comparison
- Physical and virtual machine experiments
- Large-scale heterogeneous deployment

## Experimental Evaluation

The system was evaluated using several configurations, including:

- 3 nodes on a single machine
- 10 nodes on a single machine
- 10 nodes deployed on virtual machines
- 3 heterogeneous physical machines
- DDS vs. Zenoh middleware comparison
- A larger heterogeneous deployment with 14 distributed nodes

The evaluation considers latency, throughput, and load-distribution uniformity.

## Citation

If you use this repository or build upon this work, please cite:

```bibtex
@article{alqahtani2026dds,
  title={A Distributed System Design Based on DDS Middleware for Dynamic Real-Time Load Balancing},
  author={AlQahtani, Azizah and AlAmoudi, Nadeen and AlShmrani, Manal and Helmy, Tarek},
  journal={Concurrency and Computation: Practice and Experience},
  year={2026},
  publisher={Wiley},
  doi={10.1002/cpe.70955}
}
