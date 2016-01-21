# eclair

A scala implementation of the Lightning Network. Eclair is french for Lightning.

More precisely, this is an implementation of Rusty's [deployable lightning](https://github.com/ElementsProject/lightning/raw/master/doc/deployable-lightning.pdf). In particular it uses the same wire protocol, and almost the same state machine.

## Modules
* lightning-types: scala code generation using protobuf's compiler (wire protocol)
* lightning-core: actual implementation

## Overview

The general idea is to have an actor per channel, everything beeing non-blocking.

A "blockchain watcher" is responsible for monitoring the blockchain, and sending events (eg. when the anchor is spent).

## Usage
Run `Demo.scala` to have an example of:
 1. Opening a channel
 2. Updating the balance with an HTLC
 3. Closing the channel

## Status
- [ ] Network
- [ ] Routing
- [X] Channel state machine
- [X] HTLC Scripts
- [ ] Relaying Payment
- [X] Blockchain watcher

## Ressources

- [1] Lightning Network by Joseph Poon and Thaddeus Dryja ([website](http://lightning.network)), [github repository (golang)](https://github.com/LightningNetwork/lnd)
- [2] LN-Implementation by Rusty Russel (Blockstream), [github repository (C)](https://github.com/ElementsProject/lightning)
- [3] Thunder Network by Mats Jerratsch (Blockchain.info), [github repository (Java)](https://github.com/matsjj/thundernetwork)
