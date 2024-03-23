# Digital Data Hub

## Experimental Software - Work in Progress

This experimental software implements ideas from the paper:

[M. Gfeller and T. Hardjono, _Privacy and Security Requirements for a Digital Data Hub_. TechRxiv, 10-Dec-2021](https://www.techrxiv.org/doi/full/10.36227/techrxiv.17048384.v1).

See is also the [illustrated introduction](https://www.linkedin.com/feed/update/urn:li:activity:6891015464403693568).

## Key Abstractions

### External Abstractions

#### DDHKey

The DDHkey designates data in the sense of REST. It places data on the Schema Tree. Specifiers designate specific data:

- Fork: schema, data, consent
- Variant: designates a specific schema if there are multiple
- Version: designates a specific version of a Schema



#### DataApp

#### Schema Tree

#### Schema

### Internal Abstractions

These abstractions are restricted to within the framework.

#### Node

#### KeyDirectory


## Key Services

#### Walled Garden

The Walled Garden is the user environment for DApps. The environment provides isolation between apps and from the outside. 

#### DDH API

This is both the API from the outside to read DDH data and the API that Data Apps use (within the Walled Garden) to read and write data. 

#### Market

#### DApp Recommender


### DataApp

Each DataApp is implemented as its own service


Copyright 2020-2022 by Martin Gfeller, Swissoom (Switzerland) Ltd.
