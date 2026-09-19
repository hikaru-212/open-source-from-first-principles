# Understanding Ray from First Principles

## From One `remote()` Call to Reading Your First Ray Issue

- Status: Prototype for Review
- Technical baseline: Ray 2.58.0
- Source tag: `ray-2.58.0`
- Language: English
- Author: Yen-Hua Chen
- License: CC BY 4.0
- Suggested reading time: 2–3 hours

This is a teaching prototype, not a complete Ray specification or API manual. If this guide differs from official material, the Ray 2.58.0 documentation and the source and tests under the `ray-2.58.0` tag take precedence.

`master` is evidence about development in progress, not Ray 2.58.0 behavior. GitHub issues and unmerged pull requests are not public contracts either.

## What Is Ray?

Ray is a distributed execution runtime/framework that runs tasks and stateful actors across CPUs and GPUs on multiple machines.

Users declare computations and resource requirements; Ray chooses where and on which worker to run them, allocates logical resources, tracks results, and manages object movement and lifetime. After a worker or node failure, it retries or reconstructs work according to the relevant policies and eligibility conditions. It solves the coordination problem of distributed execution; recovery does not mean continuing from the point of interruption.

Ray does not provide application-level exactly-once external effects, durable business workflow history, or database transaction semantics.

## Opening Question: What Engineering Burden Remains Without Ray?

Suppose a computation must run across CPUs and GPUs on multiple machines, with later work depending on earlier results. Without Ray, custom code or another system still has to answer: who tracks work that has not run yet and selects available resources and workers? Who moves dependencies to where they are needed and tracks who still uses each result? After a worker or node disappears, who determines which work can run again and which state can no longer be recovered?

Carry this question through the guide: which coordination of distributed execution can move into the runtime, and which application state and external effects must we still manage ourselves?

With that positioning in place, Chapter 1 asks: what actually exists after `f.remote()`?

Throughout the guide, we then repeatedly ask:

> What physically exists now? Who knows what? Which physical state disappeared? Who can recover it?

## How to Read This Guide

On your first pass, you do not need to memorize every Ray internal name. In each chapter, answer only three questions first:

1. What logical state exists now?
2. Which physical component is doing the work?
3. After a failure, what remains and what disappears?

Save the `Deep Dive`, source files, and issue numbers for a second pass.

---

## Chapter 1: What Actually Exists After a `remote()` Call?

Start with an ordinary Ray call:

```python
@ray.remote
def f(x):
    return x * 2

ref = f.remote(21)
```

When `f.remote(21)` returns, has the function already run? Has a worker received the work? Does `ref` prove that the answer has been computed?

Not necessarily.

### Keep This One Sentence in Mind

> A task is the logical identity of work, not a process.

What we can say is that this `.remote()` call has created and submitted a trackable logical task and returned an `ObjectRef` for its future result. The `ObjectRef` means that the result can be obtained through this reference; it does not mean that user code has run.

The most common incorrect intuition is to treat a task as one fixed process:

```text
Incorrect intuition:

one remote call
      ↓
one fixed worker
      ↓
one result
```

In reality, we need three layers:

```text
f.remote()
    ↓
logical task
    ↓
attempt 0 ── worker W1
    │
    └── failure
          ↓
        attempt 1 ── worker W2
```

A task is a logical identity. An attempt is one physical execution attempt. A worker is the process that executes that attempt. When a failure is eligible for retry, the same task can produce another attempt, and that attempt does not have to run on the original worker.

The reverse is also true: an ordinary task worker can usually execute many unrelated tasks in sequence. Therefore:

> Logical work is not the same thing as a physical worker.

Actors require the same separation between logical identity and physical process. An actor is an abstraction with identity and in-memory state; a dedicated actor worker physically hosts it.

If an actor is allowed to restart, Ray can preserve the same logical actor identity, create a new worker process, and run the constructor again:

```text
Actor A
  ├── incarnation 1: worker W1, heap H1
  └── restart
         ↓
      incarnation 2: worker W2, a new heap H2
```

What survives is the actor identity and the relationship represented by its handles—not the old process's stack, heap, or application state.

**What Ray guarantees at this boundary:** A `.remote()` call creates a trackable logical task and result reference. A retry can create a new attempt for that same task.

**What this does not establish:** That a worker has been selected, user code has started, a result exists, or no external side effect has occurred.

### Checkpoint

`f.remote()` has returned an `ObjectRef`, but you have not called `ray.get()`. Which logical states now exist, and what remains unknown about the worker, user code, and result?

### Deep Dive (Optional)

- `TaskID`, attempt number, `WorkerID`, and `ActorID`
- Actor incarnations and intended-recipient worker checks

---

## Chapter 2: How Many Steps Separate Resource Demand from Execution?

Suppose Ray has received a logical task. The next common intuition is:

> If resources exist, the task runs. If it is scheduled, it must be running.

That intuition compresses several different physical transitions into one word.

### Keep This One Sentence in Mind

> Scheduled does not mean executing.

You submit a task that requires 8 CPUs, but it remains pending. Perhaps no node in the cluster can provide 8 CPUs at all. Or perhaps a suitable node exists, but other work currently occupies its resources. Both look pending, but the system can take very different next steps.

From submission until a result becomes observable, a task moves through a path roughly like this:

```text
submission
    ↓
dependency readiness
    ↓
resource feasibility
    ↓
worker lease + logical resource allocation
    ↓
dispatch
    ↓
user-code execution
    ↓
result publication
```

**Submission** means that Ray has received the task specification. An `ObjectRef` may already have been returned, but the task has not necessarily executed.

If the arguments contain other `ObjectRef`s, Ray must also wait until those dependencies can be obtained. This is **dependency readiness**, not CPU scheduling.

Next, separate two questions that are easy to conflate:

- **Feasible:** Does any node have a resource shape capable of running this task?
- **Currently available:** Are those logical resources free now?

For example, a task requesting 8 CPUs is infeasible if every node has only 4 CPUs. If one node has 8 CPUs but they are currently occupied, the task is feasible but unavailable. Both tasks can be pending for physically different reasons.

When a raylet finds a placement, Ray allocates logical resources and grants a worker lease. The submitting side must still dispatch the task. After receiving it, the worker still needs to resolve references and deserialize the function and arguments before entering user code.

Successful scheduling therefore establishes at most this:

> At that moment, Ray found a feasible placement, allocated logical resources, and selected the intended worker.

It does not establish that dispatch arrived, user code started, execution completed, or the result reached the caller.

### Logical CPU Is Not CPU Pinning

Ray's `num_cpus=1` is admission control. It limits how many logical resources Ray allocates concurrently, but it does not mean that:

- The process is pinned to one physical CPU.
- The operating system permits it to use only one CPU.
- User code cannot create additional threads.
- Workers receive hardware-level isolation from one another.

### Placement Groups: Reservation Is Not Execution

A placement group is an advanced reservation example: it allocates multiple logical resource bundles together. Initial creation uses gang/atomic reservation. All bundles must be reservable for creation to succeed; otherwise no bundle is reserved.

`pg.ready()` therefore proves that the initial reservation completed. It does not prove that an actor initialized, a task started, or the application produced a useful result.

**What Ray guarantees at this boundary:** Logical resources govern how Ray admits and allocates work. Initial placement-group creation provides atomic reservation of its bundles.

**What this does not establish:** Physical CPU isolation, entry into user code, or an execution result produced by the reservation.

### Checkpoint

A task has obtained a worker lease, and Ray has allocated its required logical CPUs, but the worker is still handling dispatch and argument deserialization. What has “scheduled” established at this point, and what has it not established?

### Deep Dive (Optional)

- Placement-group bundle scheduling and failure recovery
- Spillback
- GPU visibility
- Blocking calls and logical CPU accounting

---

## Chapter 3: Who Owns the Result, and Where Are Its Bytes?

Consider this situation:

> A driver creates a task, a worker on another machine executes it, and the result bytes end up in a third location. Who does that result actually “belong to”?

### Keep This One Sentence in Mind

> Owner, executor, and byte storage location are three different things.

```text
driver D
   │ creates the task and receives an ObjectRef
   ▼
worker W
   │ executes the function
   ▼
object store on node N
   stores the result bytes
```

Which role “owns” this object?

The intuitive answer might be the node that stores the bytes. In Ray, however, ownership is not the same as storage location.

For an ordinary remote task, the worker that submits the work and creates the original `ObjectRef` is the owner. The worker that runs user code is the executor. The result bytes may live in a worker's memory, an object store on another node, or several replicas.

Keep this distinction fixed:

```text
owner
  != executor
  != byte storage location
```

The owner maintains lifecycle information for the logical object. The executor knows what it executed. Object storage holds the bytes. These roles may occasionally be located in the same process or node, but their responsibilities differ.

### ObjectRefs and Lifetime

As long as a trackable `ObjectRef` exists, distributed reference tracking lets Ray know that the object remains in use. A reference may also appear as an argument to a pending task or inside another Ray object.

Beginners do not need the complete distributed reference-counting protocol yet. The important point is:

> An `ObjectRef` is not just an address; it also participates in Ray's decisions about object lifetime.

### Losing Bytes Does Not Necessarily Mean Losing the Logical Object

If one object replica disappears with a node, Ray first looks for another copy. If no copy exists and the result originally came from an eligible task, Ray may be able to use lineage to run the producer task again:

```text
object bytes lost
      ↓
another replica?
  ├── yes → use the replica
  └── no
       ↓
producer lineage usable?
  ├── yes → execute the producer again
  └── no  → object loss
```

This does not restore bytes from a backup. It repeats the work that produced them, so the producer's external side effects may occur again.

`ray.put()` does not have the same producer-task reconstruction path. It stores a value that already exists, so there is no original producer task that Ray can naturally rerun.

Ordinary tasks have their own retry and reconstruction eligibility. Actor methods default to `max_task_retries=0`, so under that default, objects produced by actor tasks are not reconstruction candidates. Configuring a nonzero actor-method retry policy changes that eligibility.

### Owner Loss Is a Different Failure

If only a byte replica disappears while the owner, other replicas, or producer lineage survive, Ray may be able to recover the object.

If the owner dies, the logical object's metadata and reconstruction authority disappear as well. Even if some bytes once existed on another node, that does not mean ownership still exists intact.

**What Ray guarantees at this boundary:** The owner coordinates the logical object; bytes can live elsewhere; eligible task-produced objects can use lineage reconstruction.

**What this does not establish:** That the node storing the bytes is the owner, every object can be reconstructed, or owner death is equivalent to losing one replica.

### Checkpoint

If an object's only byte replica disappears while the owner and producer lineage survive, what might Ray reconstruct? Why is the answer different if the bytes remain but the owner dies?

### Deep Dive (Optional)

- Nested references
- Out-of-band `ObjectRef` serialization
- Lineage retention and eviction
- Detached actors and detached placement groups

---

## Chapter 4: When a Process or Node Disappears, What Is Actually Lost?

“A machine failed, so Ray will recover it” is not precise enough. A node may simultaneously host task workers, actor workers, object bytes, and placement-group bundles. Different mechanisms handle each of them.

### Keep This One Sentence in Mind

> “The node died” is not one failure; it is several kinds of state disappearing at once.

Imagine one node running an ordinary task and an actor while also storing an object replica and a placement-group bundle. The entire node disappears. The question is not simply “Will Ray retry?” but what each of those four things has lost.

| Failure | Physical state lost immediately | Logical identity that may remain | Who may initiate recovery | What cannot be restored automatically |
| --- | --- | --- | --- | --- |
| Task worker death | That attempt's stack and heap | `TaskID` and result refs | Task retry path | Process-local state and external effects |
| Actor worker death | Actor heap and active call stack | `ActorID`, if restart is allowed | Actor lifecycle | Application state that was not explicitly saved |
| Node death | Worker state and unavailable replicas on that node | Depends on each entity's policy | Tasks, actors, objects, and PGs recover separately | A consistent snapshot of the entire node |
| Owner death | Ownership metadata and recovery authority | A detached entity may survive independently | Usually termination and cleanup | Reconstruction authority for owned objects |

### Worker Death: One Attempt Is Lost

If a task worker crashes in user code, the old attempt's stack and heap disappear immediately. If retry budget remains, Ray can create a new attempt for the same logical task.

External writes completed by the old attempt are not undone when its process disappears.

### Actor Worker Death: Restart Is Not State Restoration

If an actor may restart, Ray can create a new process and run its constructor again. The old actor heap is not moved to the new worker.

```text
actor worker dies
       ↓
old heap disappears
       ↓
new process starts
       ↓
constructor runs again
```

Application state returns only if the program explicitly loads it from durable storage or a checkpoint.

### Node Death: Several Recovery Paths Begin

Node failure does not trigger one all-purpose “node rollback.” It may simultaneously cause:

- A task attempt to enter task retry.
- An actor's restart policy to decide whether it is recreated.
- An object to use a replica or reconstruction.
- Lost placement-group bundles to wait for new placement.

These policies are independent. A placement group's surviving bundles can remain reserved while lost bundles recover separately. Atomic initial reservation does not imply that the entire group must succeed or fail together after a fault.

### Owner Death: Reaching Termination Is Itself Distributed

Owner death is not merely one process disappearing. It can also trigger distributed cleanup of resources, references, worker leases, and pending work.

Different components may participate in cleanup, so reaching a terminal state is itself a distributed transition that must converge, not one instantaneous local action.

**What Ray guarantees at this boundary:** Different logical entities use their own policies to create a new attempt, process, object producer, or bundle placement.

**What this does not establish:** Restoration of old process state, a consistent restoration of the entire node, or one recovery policy shared by every entity.

### Checkpoint

One node hosts task T, actor A, the only replica of object O, and placement-group bundle B. When the node disappears, why is “Will Ray retry?” the wrong single question? Which recovery policies must you examine separately?

### Deep Dive (Optional)

- GCS and head-node recovery
- Partial placement-group recovery
- Owner-death cleanup convergence: worker leases, resource accounting, pinned arguments, and dependency bookkeeping
- Ray #64627: the cleanup concern has implementation evidence, but the complete production leak remains unresolved

---

## Chapter 5: Recovery Is New Execution, Not Rewinding Time

This is the core of understanding Ray fault tolerance.

### Keep This One Sentence in Mind

> Recovery is new execution, not rewinding time.

Ray has several mechanisms that all look like “try again,” but they differ in their triggers, preserved identities, and repeated work:

| Mechanism | Typical trigger | Logical identity preserved | What physically happens again |
| --- | --- | --- | --- |
| Task retry | Task worker or node failure | The same logical task and result refs | A new task attempt is created |
| Actor restart | Actor worker or node failure | The same `ActorID` | A new process is created and the constructor runs again |
| Actor-method retry | Actor unavailable, dead, or restarting | The same logical actor task | Method code may execute again |
| Object reconstruction | No object bytes remain available | The same logical object | The producer task may execute again |

### Task Retry: A New Physical Attempt

A task retry preserves logical task identity, but it does not preserve the worker process's execution state.

If attempt 0 crashes after sending a network request, attempt 1 does not automatically know whether the external service accepted it. The application needs its own idempotency or acknowledgement mechanism.

### Actor Restart: Recreating the Process

`max_restarts` controls whether an actor process can be recreated. A restart runs the constructor again, so constructor side effects may also repeat.

It does not automatically restore the old heap, resume at the previous line of the failed method, or restore actor state from before that method. Whether the failed method runs again is a separate actor-method retry policy.

### Actor-Method Retry: The Method May Run Again

Even without method retry, an error observed by the caller does not necessarily prove that the method never ran:

```text
actor method writes to an external database
                  ↓
method completes successfully
                  ↓
actor dies before completion reaches the owner
                  ↓
caller receives a failure
```

If method retry is enabled, a new attempt may write again. Ray can repeat computation, but it does not create an exactly-once transaction in the external database.

### Object Reconstruction: Rerunning the Producer

Object reconstruction may execute the task that produced a result again rather than restore the original bytes directly:

```text
producer attempt 0
    ├── external effect
    └── object bytes ── lost
                         ↓
                 producer attempt 1
                    ├── external effect again?
                    └── new bytes
```

If the producer contains non-idempotent external effects, the application must handle duplicates.

Keep this boundary fixed:

```text
recovery
  != continuation
  != rollback
  != exactly-once external effect
```

A caller-visible error is one observation, not a complete execution history. The right question is not “Did Ray call this only once?” but:

> Which logical operation is being retried? Which physical attempt may have entered user code? How does the external system recognize a duplicate?

**What Ray guarantees at this boundary:** Each recovery mechanism follows its own policy to create new execution or a new process while preserving a particular logical identity.

**What this does not establish:** Process continuation, application rollback, reversal of external effects, or proof that user code never ran.

### Checkpoint

An actor method wrote an order to an external database, but the actor died before the success response arrived. The caller receives an error. Why does this not prove that the write never happened? If method retry is enabled, what kind of protection does the application still need?

### Deep Dive (Optional)

- Ray #44719: effective retry budget and restart-time policy are separate questions; the issue's proposal is not the current contract
- Actor ordering under retries
- Application idempotency, operation IDs, and fencing

---

## Chapter 6: From Ray User to Ray Contributor

Being Contribution Ready does not mean reading the entire Ray repository. It means following one bounded behavior through the evidence to the implementation responsible for it.

### Keep This One Sentence in Mind

> Contribution Ready means tracing one evidence chain, not understanding all of Ray.

Use one consistent investigation path:

```text
public behavior
      ↓
pinned stable documentation
      ↓
focused test
      ↓
owning implementation
      ↓
failure timeline
      ↓
issue diagnosis / minimal fix
```

### Pin the Version First

Before investigating, identify the Ray release the user installed, its Git tag, and whether the documentation describes that same version.

This guide is pinned to Ray 2.58.0 and `ray-2.58.0`. `master` can show the direction of development, but it cannot redefine stable behavior. An issue comment can suggest a hypothesis, but it cannot replace public documentation and stable evidence.

### Main Path: Task Retry and Object Reconstruction

Suppose a user reports:

> A task ran again after its worker died. Later, when the object's bytes were lost, the producer ran yet again.

First split that report into two public questions:

1. When does task worker failure trigger a retry?
2. When does object loss trigger producer reconstruction?

Then investigate in order:

1. Read the Ray 2.58 task and object fault-tolerance documentation. Identify the conditions involving retry budget, owner, replicas, and lineage.
2. Read a focused test in `test_reconstruction.py`. Observe how a task-produced object is reconstructed and how it differs from `ray.put()`.
3. Enter the C++ `core_worker` layer under `src/ray/core_worker/`. Use `TaskManager` and `ObjectRecoveryManager` to find retry accounting and the recovery decision.
4. Draw a failure timeline. Identify who drives each transition, which state it uses, and what the caller observes on failure.

```text
ObjectRef still live
      ↓
bytes unavailable
      ↓
another replica?
      ↓
lineage eligible?
      ↓
producer retry or terminal error
```

A focused test establishes that the stable repository explicitly protects one case; it does not establish the same guarantee for every neighboring situation. Source code shows how the implementation works, but not every implementation detail is a public contract.

The State API is diagnostic evidence. Its results can be incomplete because a data source is unavailable, a query is limited, or records have been garbage-collected. It also does not guarantee one globally consistent snapshot. Therefore, failing to find a task does not mean it never existed, and seeing `RUNNING` does not establish the state of an external side effect.

An investigation can still contribute even without a patch: it can reduce a reproduction, add a focused test, clarify a documentation boundary, or reveal two state machines that had been conflated.

> Contribution Ready does not mean knowing all of Ray.
> It means tracing one bounded behavior from the public contract to evidence, and then to the implementation responsible for that state transition.

**Evidence boundary established by this investigation path:** Stable documentation, tests, and source can build an inspectable evidence chain for one specific version.

**What this does not establish:** That master equals stable, an issue comment equals a contract, or one State API record represents complete runtime truth.

### Checkpoint

You receive an issue claiming that a producer unexpectedly ran twice after node failure. Before changing source, how should you pin the version, establish the public contract, find a focused test, separate task retry from object reconstruction, and decide what the State API output can actually prove?

### Deep Dive (Optional)

- Actor restart → actor failure test → `ActorTaskSubmitter` / `ActorManager`
- Placement-group failover → focused failover test → GCS placement-group manager; the #65147 reproduction has not been independently confirmed on 2.58, and PR #65970 remains unmerged
- #44719: part of the policy layering is verified, but its proposed behavior is not the current contract
- #64627: the cleanup-path concern has source evidence, but the complete production leak remains unresolved

---

## Closing Synthesis: What Responsibility Does Ray Take Off Our Plate?

Return to the computation spanning multiple machines. Ray takes on the runtime work of coordinating logical work into physical execution attempts, so each application does not have to build its own distributed execution coordinator. We can now connect that responsibility to the mechanisms we examined:

- **Task/actor identity and execution attempts** make work trackable without permanently binding it to one worker process; a task is still not a worker.
- **Scheduling, logical resources, and dependency readiness** coordinate placement, resource allocation, and the data conditions for execution; scheduled does not mean user code has started, and logical CPUs do not provide hardware isolation.
- **ObjectRefs, owners, and reference tracking** coordinate result tracking, movement, and lifetime; owner, executor, and byte location remain three distinct roles.
- **Task retries, actor restarts, and object reconstruction** create new attempts or processes, or rerun producers, under their respective policies and eligibility conditions; they do not continue the failed process's stack or heap or automatically restore business state.

Applications still define computations and resource requirements, persist actor state that must survive, and handle idempotency and transaction semantics for external effects. Operators still supply and manage cluster resources. Taking on the coordination of distributed execution does not make Ray a durable business workflow history or establish exactly-once completion of external business operations.

---

## Authoritative Sources by Chapter

The `docs.ray.io/en/latest` pages below corresponded to Ray 2.58.0 when this prototype was verified, but the URLs themselves move with the current stable release. Fixed implementation evidence should use the `ray-2.58.0` tag.

### Chapter 1

- [Ray Tasks](https://docs.ray.io/en/latest/ray-core/tasks.html)
- [Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html)
- [common.proto — task attempt identity](https://github.com/ray-project/ray/blob/ray-2.58.0/src/ray/protobuf/common.proto)

### Chapter 2

- [Resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)
- [Placement Groups](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html)
- [`task-lifecycle.rst`](https://github.com/ray-project/ray/blob/ray-2.58.0/doc/source/ray-core/internals/task-lifecycle.rst)

### Chapter 3

- [Object Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html)
- [ObjectRef Reference Counting](https://docs.ray.io/en/latest/ray-core/scheduling/memory-management.html)
- [`object_recovery_manager.cc`](https://github.com/ray-project/ray/blob/ray-2.58.0/src/ray/core_worker/object_recovery_manager.cc)

### Chapter 4

- [Task Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html)
- [Actor Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)
- [Node Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/nodes.html)
- [`test_placement_group_failover.py`](https://github.com/ray-project/ray/blob/ray-2.58.0/python/ray/tests/test_placement_group_failover.py)

### Chapter 5

- [Task Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html)
- [Actor Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)
- [`task_manager.cc`](https://github.com/ray-project/ray/blob/ray-2.58.0/src/ray/core_worker/task_manager.cc)
- [`actor_task_submitter.cc`](https://github.com/ray-project/ray/blob/ray-2.58.0/src/ray/core_worker/transport/actor_task_submitter.cc)

### Chapter 6

- [`test_reconstruction.py`](https://github.com/ray-project/ray/blob/ray-2.58.0/python/ray/tests/test_reconstruction.py)
- [`TaskManager`](https://github.com/ray-project/ray/blob/ray-2.58.0/src/ray/core_worker/task_manager.cc)
- [`ObjectRecoveryManager`](https://github.com/ray-project/ray/blob/ray-2.58.0/src/ray/core_worker/object_recovery_manager.cc)
- [Ray State API](https://docs.ray.io/en/latest/ray-observability/user-guides/cli-sdk.html)

### Case Studies: Not Ray 2.58 Public Contracts

- [Ray #44719](https://github.com/ray-project/ray/issues/44719): parts of the structural analysis are supported by source; the proposed semantics are not the current contract
- [Ray #65147](https://github.com/ray-project/ray/issues/65147): the structural concern remains relevant; it has not been independently reproduced on Ray 2.58
- [Ray PR #65970](https://github.com/ray-project/ray/pull/65970): unmerged
- [Ray #64627](https://github.com/ray-project/ray/issues/64627): the cleanup concern has implementation evidence; the complete production leak remains unresolved

---

## Methodology Provenance and Verification

The first-principles teaching method in this guide grew from the engineering reasoning documented by Yen-Hua Chen in *Streaming System + Compass*: identify physical actors, state transitions, evidence boundaries, and failure timelines before discussing abstractions and APIs.

- **Streaming System + Compass — Yen-Hua Chen**
- [https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)
- Documentation license: CC BY 4.0

The Compass architecture is not directly applied to Ray. Ray-specific claims were independently checked against Ray 2.58.0 documentation, tagged source, and focused stable tests. Analogies were explicitly rejected where the physical mechanisms did not match:

- Ray actor != Compass semantic authority
- Lineage reconstruction != event-log replay
- GCS != application source of truth
- Placement-group reservation != distributed transaction
- Actor reincarnation != ownership transfer

---

## Author Note

This short main path deliberately omits Ray product tours, complete GCS/raylet/scheduler internals, reference-counting edge cases, the full placement-group recovery algorithm, and issue conclusions that lack stable evidence.

A future Expanded/Deep Dive edition can address actor retry ordering, partial placement-group recovery, owner-death cleanup convergence, application fencing, and the complete contribution workflow from reproduction to regression test and minimal patch.
