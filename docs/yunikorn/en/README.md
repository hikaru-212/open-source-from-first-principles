# Understanding YuniKorn from First Principles

## From “Who Should Get the Resources?” to Reading Your First YuniKorn Issue

- Status: Prototype for Review
- Technical baseline: Apache YuniKorn 1.9.0
- Mode: standard scheduler mode
- Language: English
- Author: Yen-Hua Chen
- License: CC BY 4.0
- Suggested reading time: 2–3 hours

This is a teaching prototype. It is not a complete YuniKorn specification, a Kubernetes scheduler textbook, or a configuration manual. If this guide conflicts with another source, defer to the Apache YuniKorn 1.9.0 documentation and to the pinned core and Kubernetes shim source and stable tests cited here.

## What Is YuniKorn?

YuniKorn is a multi-tenant resource scheduler for Kubernetes.

When applications and queues compete for finite CPU, memory, and other cluster capacity, it applies policy to decide who is considered first, which resource demand can receive an allocation, which feasible node should host it, and which existing allocations may be preempted when necessary. It answers “who should receive the limited resources?” Being considered first does not establish feasibility, and selecting a preemption victim does not mean its resources have been released.

YuniKorn does not create containers or execute processes. A scheduler decision alone cannot prove that a workload has begun useful computation.

With that positioning in place, Chapter 1 asks: what is actually being allocated?

## Keep This Question in Mind

> Without YuniKorn, or another scheduler responsible for shared-cluster resource policy, when applications, teams, and queues compete for finite CPU, memory, and other cluster capacity, who decides who gets resources first, how much they can receive, which requests must wait, and when occupied resources should be reclaimed?

## How to Read This Guide

On your first pass, you do not need to memorize every internal name. Track only four questions in each chapter:

1. Who is requesting resources now?
2. Which component has the authority to decide what?
3. Where can scheduler state differ from Kubernetes or runtime state?
4. After a failure, which transition is still incomplete?

You can leave `Deep Dive` sections, exact source paths, and issue numbers for a second pass.

---

## Chapter 1: What Is Actually Being Allocated?

Suppose a data-processing job creates three Pods. They belong to the same job, but each has its own Pod UID and declares different CPU and memory requests:

```text
Application analytics-job
  ├── Pod UID p-101: 1 CPU, 1 GiB memory
  ├── Pod UID p-102: 2 CPU, 2 GiB memory
  └── Pod UID p-103: 1 CPU, 4 GiB memory
```

We might intuitively say, “YuniKorn is scheduling these three Pods.” That is not wrong, but it is incomplete. YuniKorn must also know whether the Pods belong to the same unit of work, how many resources each Pod requests, which requests are still waiting, and which requests have been placed on a node.

### Remember This

> YuniKorn allocates declared resource demand, not CPU utilization itself.

Begin with a minimal model:

| Name | Meaning in this guide |
|---|---|
| application | The scheduler-level logical grouping of related work; it is not a Kubernetes API object |
| Pod/task | In the standard shim, one Pod corresponds to one shim task |
| pending request/ask | A resource request still waiting for node placement |
| allocation | The core has placed the request on a node and updated scheduler accounting |
| node | The scheduler-side model of a Kubernetes Node that can host allocations |
| resource vector | A multidimensional request such as `{cpu: 2, memory: 2 GiB}` |

One application can contain multiple Pods. The standard shim uses the Pod UID as the task ID and as the allocation key. The example therefore has one application identity and three distinct allocation keys.

```text
application ID: analytics-job

allocation key p-101 ── pending or allocated
allocation key p-102 ── pending or allocated
allocation key p-103 ── pending or allocated
```

`ask` remains a common term in the core source, but do not teach it as a separate current message in the YuniKorn 1.9.0 scheduler interface. The 1.9.0 interface sends an `Allocation`; on the ordinary core path, the same allocation object changes from pending to allocated while its allocation key stays the same.

This gives us two relationships that must remain separate:

```text
application != Pod
pending demand != allocated placement
```

A resource request is an accounting fact. It says, “Account for these resources if you place this Pod.” It does not report how much CPU the process is using now, nor its resident memory. Actual utilization may be much lower or may fluctuate; the scheduler still makes placement decisions from requests, allocatable capacity, and related data.

**What YuniKorn can establish at this layer:** which application submitted which request, the resource dimensions in that request, and whether the scheduler currently considers it pending or allocated to a node.

**What does not follow:** that a node has enough physical room, the Pod is bound, a container has started, or the application has made real progress.

### Checkpoint

Three Pods share one application ID but have different UIDs. If one allocation exists while the other two remain pending, which identity is shared? Which states must be tracked separately? How far does the resource request prove anything?

### Deep Dive (Optional)

- How scheduler-interface 1.9.0 represents pending and allocated information in the `Allocation` message
- Extended resources and multidimensional resource vectors
- How Pod overhead enters request calculation
- Foreign allocations: how Pods not scheduled by YuniKorn enter node accounting

---

## Chapter 2: What Happens Between a Pod Appearing and YuniKorn Making an Allocation?

Suppose a user submits a Pod. Later, YuniKorn selects `node-a`. Does that mean the Pod has started running?

It does not. Several authority boundaries lie between the API request and a real process:

```text
Kubernetes Admission
        ↓
Pod object exists in the API server
        ↓
YuniKorn application registration / queue placement
        ↓
pending resource demand
        ↓
core allocation: select node, update accounting
        ↓
shim performs bind
        ↓
kubelet admission / container runtime execution
```

### Remember This

> An allocation does not mean the Pod is already running.

`Admission` has a specific Kubernetes meaning: before an API request is persisted, admission plugins or webhooks mutate and validate it. To avoid confusion, this guide does not shorten YuniKorn application acceptance to “admission.”

When we say “YuniKorn accepts an application registration,” the safe meaning is that the core found the partition, completed application placement, registered the application in a leaf queue, and told the shim that registration was accepted. It does not mean the application has a pending request, much less that resources are available.

The shim then translates the Pod UID, resource request, priority, and constraints into a request the core can process. The core applies queue policy and node feasibility to make scheduling decisions, while maintaining application, queue, and node accounting.

On the ordinary new-allocation path, a successful core allocation supports these conclusions:

- It selected a node in that scheduling decision.
- The request passed the queue, user, and node resource checks at that time.
- The predicate check supplied by the shim accepted the placement.
- The core accounted for the request on the selected node and queue.
- The allocation response returned the node decision to the shim.

The later steps can still fail. The shim must handle volume binding and issue a Pod bind through the Kubernetes API. Even after a successful bind, the kubelet must admit the Pod on the node, and the container runtime must create a sandbox, obtain images, and start containers.

For example:

```text
core selects node-a
        ↓ success
shim binds volumes
        ↓ failure
Pod does not start executing
```

Or:

```text
Pod is bound to node-a
        ↓
kubelet cannot start the container
        ↓
an allocation existed, but the workload did not begin useful work
```

Each component can prove something different:

| Component | Facts it primarily owns | What it cannot establish alone |
|---|---|---|
| scheduler core | Scheduling, queue policy, and allocation accounting | API bind or process execution |
| shim | Pod/task translation, callbacks, and Kubernetes API operations | That a container is actually running on a node |
| API server | Kubernetes object and bind state | That a process has started or exited |
| kubelet/runtime | Node-local admission and container execution | That core accounting has converged |

Shim task states such as `Allocated` and `Bound` are useful when following source code, but they are implementation details rather than a public contract beginners need to memorize.

**The bounded guarantee of a YuniKorn allocation:** the core made and recorded a node-placement decision.

**It does not guarantee:** successful bind, kubelet admission, a container reaching Running, application initialization, or useful workload output.

### Checkpoint

Monitoring shows that the core selected a node for a Pod. Which scheduler facts are now established? If a user says, “YuniKorn reported success, but my program did not run,” what should you inspect next in the shim, API server, and kubelet?

### Deep Dive (Optional)

- `Allocated` and `Bound` in the shim task FSM
- `AssumePod` in the scheduler cache
- Why volume binding happens after core allocation
- How a bind failure leads back to allocation release

---

## Chapter 3: How Do Multiple Workloads Share Finite Resources?

Two teams now share one cluster:

```text
root
  ├── queue-a: Team A applications
  └── queue-b: Team B applications
```

Queue A and Queue B both want the same CPU and memory. Permanently assigning half the machines to each team would strand idle capacity. At the other extreme, pure first-come, first-served access could leave one team without resources for a long time.

### Remember This

> Guaranteed resource does not mean reserved hardware.

YuniKorn expresses resource-sharing policy through hierarchical queues. Applications are placed in leaf queues; a parent queue's usage aggregates the usage of all its descendants. The hierarchy can represent sharing for both a department and the teams inside it, but it is not a physical topology and does not mean a queue owns particular nodes.

First separate the two most important quantities:

| Concept | Boundary it establishes | What it is not |
|---|---|---|
| maximum resource | A hard cap on allocations held concurrently by a queue subtree | A promise that the capacity exists |
| guaranteed resource | A policy baseline used by fair ordering and ordinary preemption | Reserved machines, an SLA, or a completion deadline |

Suppose the cluster has 100 CPUs:

```text
queue-a guarantee = 40, max = 80, current usage = 20
queue-b guarantee = 40, max = 80, current usage = 70
```

Queue A has not converted its 40-CPU guarantee into a pool of idle hardware. While A demands only 20 CPUs, B can use more capacity when a usable node exists and B remains below its maximum. For teaching purposes, we can call the 30 CPUs that B uses above its guarantee “borrowing,” but the system creates no named physical-resource loan and no ownership claim that a particular CPU belongs to A.

The guarantee affects fair queue sorting. When queues have pending demand, the core compares usage relative to their guarantees; a queue with lower relative usage is usually considered first. A parent queue always sorts its children with the fair policy.

If A later demands more resources, its guarantee establishes a policy position: A is below its baseline while B is above its baseline. That can give A a better place in fair ordering and, when priority, preemption fences, single-node fit, and other conditions hold, can allow YuniKorn to look for preemption victims in a queue above its guarantee.

The guarantee does not create capacity. A may still make no progress if every node is unsuitable, total guarantees exceed cluster capacity, the maximum-resource hierarchy blocks placement, or reclamation fails.

**The policy boundaries YuniKorn establishes:** maximum resource limits allocations; guaranteed resource participates in fair-share calculations and supplies a protection and reclamation baseline for ordinary inter-queue preemption.

**What does not follow:** that a queue owns hardware, that falling below its guarantee produces resources immediately, or that every pending request completes within bounded time.

### Checkpoint

Queue A is below its guarantee and Queue B is above its guarantee. What policy advantage does A receive? If A's Pod still fits on no node, what has the guarantee failed to prove?

### Deep Dive (Optional)

- Multidimensional fairness when CPU and memory both matter
- How a parent constrains a child queue's effective guarantee and maximum
- The difference between user/group headroom and queue headroom
- How priority and fair ordering jointly determine consideration order

---

## Chapter 4: Why Can “Fair” Work Still Fail to Run?

Consider a cluster that appears to have 4 CPUs free:

```text
node-a: 2 CPUs free
node-b: 2 CPUs free

pending Pod: requires 4 CPUs
```

The cluster does have 4 free CPUs in aggregate. But the Pod cannot be split across two nodes; one node must provide all 4 CPUs.

### Remember This

> Fairness decides who is considered first; feasibility decides whether the request can fit at all.

Four questions must remain separate:

| Question | What does it ask? |
|---|---|
| accounting consistency | Do pending, allocated, and node-usage records agree with one another? |
| fairness | Which of several competing requests is considered first? |
| feasibility | Can the request pass resource and other predicates on one node? |
| liveness | As time and events advance, does the request eventually make progress? |

Fair ordering can correctly place this 4-CPU request first, but it cannot combine two 2-CPU fragments into one usable node. Accounting can also be completely correct: both nodes have 2 CPUs free and the request needs 4. No number conflicts with another, yet the request still cannot be allocated.

This is the physical reason YuniKorn needs reservation. If a large or constrained request cannot obtain a complete slot, smaller requests may repeatedly consume every newly released fragment first. After the larger request has waited, YuniKorn can create a reservation for it on an eligible node, try it first later, and prevent unrelated YuniKorn allocations from entering that node.

```text
reservation = “this request has priority for this node”

reservation != allocation
reservation != CPU or memory already released
reservation != Pod bind
```

A reservation records a relationship among an application, an allocation key, and a node. It does not evict an existing workload or pretend that the node's `availableResource` has increased. The reserved request can be allocated only after an existing allocation is actually released or other real capacity appears.

Reservation is a progress mechanism: it reduces the risk that later small requests will continually overtake a large request. It is not a completion theorem. A reservation can still be removed when its request, application, or node disappears, and it can still be affected by headroom, predicates, timeouts, or other state changes.

**The bounded guarantee of fairness:** policy determines the order in which work is considered.

**What neither fairness nor reservation guarantees:** that a feasible node exists or that a workload completes in bounded time.

### Checkpoint

A request was fairly ordered first and has a reservation, but remains pending. List the scheduler state the reservation establishes and the physical facts that are still absent. Why does this state not show that fairness is broken?

### Deep Dive (Optional)

- Reservation cancellation during allocation, ask removal, and timeout
- Resource fragmentation and single-node feasibility
- Why required-node requests need different handling
- The boundary among reservation, unschedulable signals, and autoscaling

---

## Chapter 5: When Does Preemption Actually “Reclaim Resources”?

Return to Queue A and Queue B. B currently uses capacity above its guarantee, A is below its guarantee, and a high-priority request from A cannot find space. YuniKorn may decide that existing allocations must yield resources.

The most dangerous intuition is, “Once the scheduler selects a victim, the resource is free.” In reality, reclamation crosses the core, shim, API server, informer, and kubelet/runtime.

### Remember This

> A preemption decision does not mean capacity has been released.

First consider the normal successful path:

```text
core selects victim
  ↓
victim is marked preempted
  ↓
victim queue's preemptingResource increases
  ↓
core emits release request and reserves node for preemptor
  ↓
shim attempts to delete victim Pod
  ↓
Kubernetes object shows terminal/deletion change
  ↓
informer observes change; shim completes task
  ↓
shim sends release feedback to core
  ↓
core removes allocation and updates node/queue accounting
  ↓
capacity becomes reusable in the scheduler model
  ↓
preemptor can then be allocated, bound, and executed
```

`marked preempted` proves only that the core selected this victim and prevents it from being selected again as a new victim. `preemptingResource` records an accounting intent for expected reclamation. Neither removes the victim from the node's allocated resources.

Keep four boundaries explicit:

```text
marked preempted != allocation released
delete requested != Pod absent
Pod absent from API != universal proof that process exited
core allocation released != universal proof of physical process exit
```

For the core, capacity becomes schedulable again when it receives and processes release feedback and removes the victim allocation from node and queue accounting. Only the kubelet/runtime owns node-local termination truth about the physical process. With force deletion, the API server can remove the Pod object without waiting for kubelet-confirmed process termination; if the node is unreachable or disconnected from the control plane, runtime termination remains unconfirmed and the old process may temporarily continue to exist.

### A Bounded Case Study: YUNIKORN-3382

YUNIKORN-3382 identifies one specific failure path in the YuniKorn 1.9.0 shim. After the core asks to release a preemption victim, the shim calls `DeleteTaskPod()`. If the delete returns an error, that handler logs and drops the error; in the traced path, it does not retry, re-drive, or report the failure to the core.

```text
normal path:
delete succeeds
  → informer/task completion
  → release feedback
  → accounting convergence

#3382 path:
delete fails
  → error is logged and dropped
  → victim remains allocated and marked preempted
  → core receives no release feedback
  → in the fixed-capacity scenario, preemptor remains pending
```

This state can be internally consistent: the core still counts the victim that actually exists in allocated resources, so node capacity does not increase from nowhere; it also retains the preemption intent. The problem is not necessarily contradictory accounting. The problem is that no component continues to own the obligation to reconcile the failed delete, causing a liveness failure.

This should not be described as a permanent deadlock under every possible future event. The victim can later complete by itself, an external deletion can succeed, the application or node can be removed, new capacity can appear, or restart reconstruction can change the result. What #3382 supports is narrower: **in the issue's fixed-capacity scenario with no other unblocking event, progress stalls.**

Do not repeat the earlier incorrect description of the direction of `preemptingResource`. The stable 1.9.0 formulas make stale preempting resource reduce calculated preemptable resource and increase calculated remaining guarantee. That policy distortion matters during source review, but the beginner main path needs only this fact: it is not node capacity that has already been released.

**What a preemption policy decision guarantees:** the core found victims under the policy in force at that moment, recorded reclamation intent, and asked the shim to act.

**What it cannot guarantee by itself:** successful deletion, arrival of informer feedback, convergence of core accounting, or process exit.

### Checkpoint

Divide one preemption into four moments: policy decision, Kubernetes action, scheduler resource release, and physical process exit. Which component supplies evidence for each moment? If you see only `preempted=true`, what can you still not infer?

### Deep Dive (Optional)

- Stable-source audit of [YUNIKORN-3382](https://issues.apache.org/jira/browse/YUNIKORN-3382) and the missing regression test
- Victim selection, remaining guarantee, and preemptable-resource formulas
- Adding a Pod UID precondition to a retry so it cannot delete a same-name replacement Pod
- Retry ownership, backoff, cancellation, and application lifecycle
- Why Kubernetes force deletion does not directly prove process exit

---

## Chapter 6: From a Pending Pod to Your First YuniKorn Contribution

You now have a model suitable for reading source. Contribution Ready does not mean reading the entire scheduler core first. It means building a checkable evidence chain from a public symptom through identities, authority boundaries, and state transitions.

The YuniKorn core and Kubernetes shim are both written in Go. You need to follow function calls, struct fields, test setup, and callbacks, but you do not need to learn all of Go syntax or the entire Kubernetes scheduler framework first.

### Remember This

> Contribution Ready means tracing an evidence chain, not reading the whole scheduler.

This chapter follows one main path:

> **Pending demand → core allocation**

Suppose the public symptom is, “The Pod stays Pending even though the queue appears to have room.” Do not immediately search the entire scheduler or assume a fairness bug. Build the evidence in order.

### 1. Pin the Version and Symptom

First confirm that the report concerns YuniKorn 1.9.0 in standard scheduler mode. Record the Pod UID, application ID, resolved queue path, resource request, and whether the Pod already has `spec.nodeName`.

A Pod with the same name may have been recreated; its name is not enough to connect it to a core request. In the standard shim, the Pod UID is the task ID and allocation key.

### 2. Identify the Authority Boundary First

```text
Pod is not bound
  ├── did application registration succeed?
  ├── did the allocation key become a pending request?
  ├── do queue/user headroom permit it?
  ├── does one feasible node exist?
  └── did the core ever return an allocation?
```

If the core never allocated the request, stay on the scheduling path. If the core did allocate it, the problem has crossed into the shim bind or kubelet/runtime boundary. This step keeps you from fixing the wrong repository.

### 3. Build the Smallest Normal Model from Focused Tests

Start with two small core tests:

- `TestAllocateDeallocate`: observe how one request moves between pending and allocated accounting.
- `TestTryAllocateFit`: create one application, one request, and one node, then confirm which node a successful placement returns.

A test protects one bounded case. It does not prove that a production symptom has the same cause, but it shows what the smallest normal transition should look like.

### 4. Read Only the Owning Transition

The core request does not appear from nowhere. The standard shim constructs a task and associated metadata from a Kubernetes Pod; [`Task.updateAllocation()`](https://github.com/apache/yunikorn-k8shim/blob/1a59488d09e8f39cbf3d07b21b1d56877c8d4c70/pkg/cache/task.go#L288-L334) then builds a scheduler-interface allocation request and sends it to the core, which uses the application ID to route the request to the corresponding application.

Then follow this short source path:

```text
Application.AddAllocationAsk
        ↓
Application.tryAllocate
        ↓
Application.tryNode
        ↓
Node.TryAddAllocation
        ↓
PartitionContext.allocate
```

Do not trace every line. Track four state changes:

1. Where does pending resource increase?
2. Where are node fit and predicates checked?
3. When is the request marked allocated?
4. When do application, queue, and node accounting update together?

### 5. Write a Deterministic Failure Timeline

Rewrite the symptom as a falsifiable sequence:

```text
shim observes Pod UID p-101
  → application is registered in root.team-a
  → allocation key p-101 enters pending
  → fair ordering selects the application
  → every candidate node fails one specific check
  → therefore there is no core allocation response
```

If you cannot identify the specific failing check, that is the scope the next test should narrow. Reproduce that transition in a focused test before proposing a minimal fix.

### 6. Maintain Evidence Discipline

| Evidence | What it can support | What it cannot support alone |
|---|---|---|
| official docs | Intended public behavior | Every internal call order |
| pinned source | Observed 1.9.0 implementation | A permanent public contract |
| focused test | Expected behavior in one bounded scenario | Every production interleaving |
| issue report/comment | Symptom, reproduction, and hypothesis | Semantics promised by the project |
| suspicious code | A failure hypothesis worth testing | A proven root cause |

A good first contribution might be a smaller reproduction, a focused test that turns from red to green, an exact failure timeline, or a clearer evidence boundary. Each is easier to review than a broad refactor without a test.

**What this source bridge teaches you:** how to follow Pod identity into a pending request, then into the owning allocation transition and focused tests.

**What it does not require:** reading the entire queue hierarchy, every preemption algorithm, gang scheduling, or Kubernetes runtime internals.

### Checkpoint

Given a Pending Pod, how would you pin the version, confirm Pod/application/allocation identity, decide whether the problem belongs to the core or shim, choose a focused test, and turn “suspicious source” into reproducible root-cause evidence before changing code?

### Deep Dive (Optional)

- Queue fairness path: `TestSortQueuesWithGuarantees` → `sorters.go` → usage-ratio comparison
- Preemption/release path: core `TestPreemption` → shim release callback → informer completion → core removal
- Fault injection: bounded oracles for bind failures, delete failures, and node events
- #3382 regression opportunity: failed-delete re-drive, UID safety, and lifecycle cancellation

---

## Back to the Opening Question: Why Do We Need YuniKorn?

After six chapters, we can turn the opening question into concrete engineering responsibilities. Even without YuniKorn, if a shared cluster needs these resource policies, another scheduler, policy layer, or mechanism must take responsibility for them:

- Organize applications into a multi-tenant queue hierarchy, track their declared multidimensional resource demand, and enforce maximum resource limits.
- Calculate guarantee-aware fair ordering from guarantees and usage, decide which queues and applications to consider next, and check whether each request passes resource and predicate checks on a single node.
- Use reservation to reduce the risk that large or constrained requests are continually overtaken and starve; a reservation is neither an allocation nor physical capacity already freed.
- Select victims under preemption policy, track reclaim intent, coordinate release feedback, and converge node, queue, and other scheduler accounting after allocation release; marked preempted does not mean the allocation has been released.

Kubernetes does not require YuniKorn to execute Pods and can use another scheduler. YuniKorn provides a particular policy and accounting layer for shared cluster capacity: allocation order, resource limits, guarantee baselines, reservation, and reclamation flows live in one traceable scheduler model.

It does not create containers, execute processes, or own kubelet/runtime truth about physical execution. A guarantee does not mean reserved hardware, fairness does not guarantee completion, and a preemption decision does not mean capacity has been reclaimed. Even converged scheduler accounting cannot by itself prove process exit.

> YuniKorn's value is not making Pods run; it takes responsibility for the policies governing how finite shared cluster resources are allocated, protected, reclaimed, and reconciled.

If someone now asks, “How is YuniKorn different from simply placing Pods on nodes?”, can you answer in 2–3 sentences?

---

## Technical Basis and Further Reading

The YuniKorn documentation below corresponded to 1.9.0 when this prototype was written. Implementation evidence is pinned to specific core and shim commits; later stable documentation must not be used to rewrite 1.9.0 source facts retroactively.

### Chapter 1

- [YuniKorn Workloads Overview](https://yunikorn.apache.org/docs/user_guide/workloads/workload_overview/)
- [Labels and Annotations in YuniKorn](https://yunikorn.apache.org/docs/user_guide/labels_and_annotations_in_yunikorn/)
- [scheduler-interface `si.proto` — 1.9.0-0](https://github.com/apache/yunikorn-scheduler-interface/blob/v1.9.0-0/si.proto)
- [core `allocation.go` — pinned 1.9.0 source](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/allocation.go)
- [shim `metadata.go` — Pod UID/task identity](https://github.com/apache/yunikorn-k8shim/blob/1a59488d09e8f39cbf3d07b21b1d56877c8d4c70/pkg/cache/metadata.go)

### Chapter 2

- [YuniKorn Architecture](https://yunikorn.apache.org/docs/design/architecture/)
- [core application registration handling](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/context.go)
- [shim allocation callback](https://github.com/apache/yunikorn-k8shim/blob/1a59488d09e8f39cbf3d07b21b1d56877c8d4c70/pkg/cache/scheduler_callback.go)
- [shim task binding path](https://github.com/apache/yunikorn-k8shim/blob/1a59488d09e8f39cbf3d07b21b1d56877c8d4c70/pkg/cache/task.go)
- [Kubernetes Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Kubernetes Kubelet Sync Loop](https://kubernetes.io/docs/reference/node/kubelet-sync-loop/)

### Chapter 3

- [Partition and Queue Configuration — resource semantics](https://yunikorn.apache.org/docs/user_guide/queue_config/)
- [Sorting Policies](https://yunikorn.apache.org/docs/user_guide/sorting_policies/)
- [Preemption Design](https://yunikorn.apache.org/docs/design/preemption/)
- [core queue sorter](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/sorters.go)
- [`TestSortQueuesWithGuarantees`](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/sorters_test.go#L34)

### Chapter 4

- [YuniKorn Resource Reservation](https://yunikorn.apache.org/docs/get_started/core_features/#resource-reservation)
- [core reservation object](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/reservation.go)
- [core node reservation handling](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/node.go)
- [`TestBasicReservation` and `TestReservationForTwoQueues`](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/tests/reservation_test.go#L83)
- [Preemption tests for victims split across nodes](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/preemption_test.go)

### Chapter 5

- [YuniKorn Preemption Design](https://yunikorn.apache.org/docs/design/preemption/)
- [core preemption transition](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/preemption.go)
- [core allocation release/accounting convergence](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/partition.go#L1476)
- [shim core-initiated release handler](https://github.com/apache/yunikorn-k8shim/blob/1a59488d09e8f39cbf3d07b21b1d56877c8d4c70/pkg/cache/application.go#L643)
- [shim informer completion path](https://github.com/apache/yunikorn-k8shim/blob/1a59488d09e8f39cbf3d07b21b1d56877c8d4c70/pkg/cache/context.go)
- [`TestPreemption`](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/partition_test.go#L2027)
- [Kubernetes Forced Pod Termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#forced-pod-termination)

### Chapter 6

- [`TestAllocateDeallocate`](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/application_test.go#L383-L428)
- [`TestTryAllocateFit`](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/application_test.go#L2304-L2330)
- [core application allocation path](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/application.go)
- [core node allocation path](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/objects/node.go)
- [core partition allocation commit](https://github.com/apache/yunikorn-core/blob/257fdef7e24b052f433a6ad074bc5a0f8381bd79/pkg/scheduler/partition.go#L876)

### Case Studies: Not Part of the YuniKorn 1.9.0 Public Contract

- [YUNIKORN-3382](https://issues.apache.org/jira/browse/YUNIKORN-3382): missing re-drive after a failed victim Pod deletion; stable source supports the bounded failure chain, but the issue remains open
- [YUNIKORN-3386](https://issues.apache.org/jira/browse/YUNIKORN-3386): preemption fault-injection scenario; this is red-by-design issue evidence, not a stable regression contract

---

## Methodology Provenance and Verification Statement

The first-principles teaching method in this guide grew from the engineering reasoning Yen-Hua Chen documented in *Streaming System + Compass*: identify physical actors, state transitions, authority boundaries, evidence boundaries, and failure timelines before discussing abstractions and APIs.

- **Streaming System + Compass — Yen-Hua Chen**
- [https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)
- Documentation license: CC BY 4.0

The Compass architecture is not directly applied to YuniKorn. Every YuniKorn-specific claim was independently checked against YuniKorn 1.9.0 documentation, pinned stable source, and focused stable tests. Analogies that do not match the physical mechanisms were explicitly rejected:

- queue guarantee != reserved hardware
- allocation != running Pod
- scheduler accounting != runtime truth
- restart reconstruction != durable workflow replay

---

## Author Note

This short main path deliberately omits the full scheduler-interface schema evolution, every application/task FSM state, complete preemption formulas, reservation-cancellation edge cases, HA/recovery races, gang scheduling, autoscaler internals, and user/group limit edge cases.

A future Expanded/Deep Dive edition can cover multidimensional fairness, required-node preemption, recovery reconstruction, gang placeholders, fault-injection harnesses, and the complete contribution path from a #3382 regression test to production retry design.
