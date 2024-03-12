# Nextflow Developer Docs

## Terminology / Assumptions

* `nextflow` always refers to the modified nextflow fork
* Entity: either a process or a workflow definition
* Entrypoint NF script is called `main.nf`

## Overview

### 1. Registration

* We subprocess `nextflow run main.nf -latchRegister` from python. This will
  * Regex `main.nf` for any `include` statements and recursively parse the imported files first.
    * Add any imported entities to a global entity map.
  * Parse entities:
    * for Process entities, note the process name, the output name(s), and add a Process Entity to the global entity map.
    * for Workflow entities, note the workflow name, input/output name(s),  parse the workflow body (see below), and add a Workflow entity to the global entity map.
    * Note entities are parsed in order of definition - in particular this forces workflow definitions to be after the definitions of the processes that they call (see below for more in-depth explanation).
      * todo(ayush): fix this
* Emit workflow DAG(s) as JSON.
* Read in the main workflow DAG from python, construct task definitions/dependencies from the DAG, and register the resulting workflow (this is straightforward, just lots of flyte boilerplate code).

### 2. Execution

* Nextflow tasks (for the most part) execute by calling `nextflow run main.nf` with some environment variables to govern execution. These variables include data about
  * the exact expression to execute,
  * what expression to return,
  * where to import any necessary names (needed in the case of calling a process/workflow defined in a different file), and
  * the values (from upstream tasks) to inject into the expression.
* Expressions/returns/imports are computed at registration time (see below).
* Output values are written under `.latch/task-outputs` in JSON format. These are read by the task and then passed to downstream tasks for consumption.
* Values are serialized to and from JSON for consumption by groovy using a basic groovy <-> JSON conversion.
  * Nontrivial objects must implement `Serializable` to serialize correctly, there is likely no way around this.
  * In process tasks and certain operator tasks, JSON values are walked in python first to extract any necessary file paths - these are downloaded first before running the task. Outputs are then walked, and any output paths are uploaded to Latch.
* There are a few classes of "plumbing" tasks for rearranging inputs for consumption by map-tasks, as well as for coalescing different definitions of variables (see below).

## Workflow Body Parsing

A workflow body is parsed using a basic recursive descent algorithm with some state to keep track of

* the current workflow DAG, and
* the variable name bindings in the current scope.

In addition, we rely on the global entity map to know which method calls are processes and which are subworkflows.

**Visiting** an `Expression` in the AST is the process of computing any necessary vertices (and adding them to the DAG with the correct dependencies) to produce the value(s) of the `Expression`, and updating the current scope with any new variable name bindings.

Visiting is recursive - an `Expression`'s sub-`Expression`s must be visited first to correctly determine any necessary dependencies. The result of visiting an `Expression` is a `ScopeVariable`, a wrapper around the vertices that produce the value(s) of that `Expression` with some metadata for correctly resolving property accesses.

### Binary Expressions

`BinaryExpressions` are treated differently based on their operator.

Assignment `Expression`s (`=` operator) don't produce a value (at least in Groovy), and hence they (excluding any recursive calls made) don't create any new vertices. These return a `null` `ScopeVariable`. They do however add a `ScopeVariable` (computed from visiting the right `Expression`) to the current scope bound to the name they are assigned to.

Multiple (Tuple) assignment is treated as multiple sequential assignment statements. Currently we only allow the right expression to be a literal `ListExpression` in this case, however with this scope architecture it will be easy to extend this to unpacking process/sub-workflow output.

Nextflow specifically overrides the bitwise or (`|`) operator to denote successive method calls (like a Unix pipe I guess), so we treat `|` `Expression`s as a `MethodCallExpression` (see below). Some edge cases to be aware of: passing in a process name on the left of a `|` will implicitly call the process without arguments, so `proc_1 | proc_2` will be evaluated as `proc_2(proc_1())`, not as `proc_2(proc_1)`. Moreover, if `proc_2` is called with arguments, the left `Expression` is passed as the first argument, so `expr | proc_2(arg_1, arg_2)` will be evaluated as `proc_2(expr, arg_1, arg_2)`. These two cases compose as well: `proc_1 | proc_2(arg_1, arg_2) == proc_2(proc_1(), arg_1, arg_2)`.

Nextflow also overloads the `&` operator in conjunction with the `|` operator to denote multiple processes called on the same inputs - i.e. `expr | proc_1 & proc_2` is the same as `proc_1(expr); proc_2(expr)`. These aren't supported yet in the integration, as I don't fully understand their semantics (and they are somewhat context dependent, as they seem to only make sense as the second argument to a pipe).

Nextflow also supports accessing process/subworkflow outputs via indexing (this is specifically for processes with multiple outputs). In this case we resolve the index to the specific output channel and return a `ScopeVariable` corresponding to that output.

Other common operators (i.e. every operator that has a callable method name) are handled by a custom channel operator that performs the binary operation on the inputs. This, as opposed to naively computing the `BinaryExpression`, is necessary as upstream values are injected as channels. What this means in practice is that an expression like `a + b` (where `a` and `b` are computed upstream) will be computed as `Channel.of().binaryOp(Channel.of(), "+", false)`. In general binary operations are not commutative, so since the left input must always be a Channel, some extra plumbing is necessary to allow for swapping ordering. This operation creates a new vertex, which is wrapped in a `ScopeVariable` and returned.

### Method Call Expressions

`MethodCallExpression`s are handled differently based on their method name.

Calls to `set` are treated as assignment expressions. The only difference is that we need to first parse the closure argument for the destination variable name.

Every other call is treated as follows.

We first check the method name against the global entity map to figure out if we are calling a process or subworkflow. As of now, we parse a workflow at the site it is defined, so the global entity map only contains process/subworkflows that have been defined above the workflow definition. It is a straightforward enhancement to defer workflow body parsing to after the entire file has been traversed.

Next, we visit each argument in order.

If we are calling a process

WIP
