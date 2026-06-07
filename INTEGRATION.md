# a2a-future — Integration Map

## Overview

a2a-future is the **protocol layer** for the SuperInstance ecosystem. It defines how agents communicate, deliberate, and filter intelligence across the lattice.

## Wiring Diagram

```
lattice-crypto (secure transport)
  │
  ▼
a2a-future ◄── categorical-agents (composition algebra)
  │          ◄── spectral-fleet (capability ranking)
  │
  ├──► deliberation: proposal → discussion → vote → resolution
  ├──► intelligence filtration: rank → filter → route
  └──► agent communication: type → encrypt → transmit → decrypt → deliver
```

## Integration Points

### lattice-crypto — Secure Communication

All A2A messages are encrypted via lattice-crypto:

- **Key exchange** — Agents establish shared keys using lattice-based key exchange.
- **Encryption** — Message payloads are encrypted with post-quantum algorithms.
- **Signing** — Messages are signed to ensure authenticity and integrity.
- **Forward secrecy** — Compromised keys don't expose past messages.

a2a-future uses lattice-crypto as its transport layer. Agents never send plaintext.

### categorical-agents — Composition Algebra

Agent capabilities compose via categorical-agents:

- **Communication channels** are typed morphisms between agent interfaces.
- **Multi-agent deliberation** is a limit/colimit construction over participating agents.
- **Protocol composition** follows functor laws — composed protocols preserve typing.

a2a-future provides the messaging substrate. categorical-agents provides the algebra.

### spectral-fleet — Capability Ranking

Not all agent contributions are equal. spectral-fleet provides:

- **Spectral ranking** — Agents are ranked by their demonstrated capabilities.
- **Filtration** — In deliberation, contributions from higher-ranked agents carry more weight.
- **Dynamic re-ranking** — As agents demonstrate new capabilities, their rank updates.

a2a-future uses spectral-fleet rankings to implement intelligence filtration:

1. **Collect** — Gather all contributions on a topic.
2. **Rank** — Score each contribution by agent capability and relevance.
3. **Filter** — Present only the top-k contributions to the deliberation.
4. **Route** — Direct the resolution to the agents best positioned to act.

## Protocol Design

### Agent Communication

```
Agent A → [compose message] → [encrypt via lattice-crypto] → [transmit] → Agent B
Agent B → [decrypt] → [validate type] → [deliver to handler]
```

### Deliberation

```
Proposer → [create proposal] → Broadcast
Agents   → [discuss] → [vote] → [aggregate via categorical-agents]
Proposer → [resolution] → [enact]
```

### Intelligence Filtration

```
Source    → [emit contribution]
Filter    → [rank via spectral-fleet] → [filter top-k]
Consumer  → [receive filtered intelligence]
```
