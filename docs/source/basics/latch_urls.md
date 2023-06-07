# Latch URLs

Files and directories on Latch can be referred to in code or through the CLI using **Latch URLs**.

## Grammar

The basic structure of a Latch URL is `latch://`, followed by a (possibly empty) **Domain**, finally ending with an absolute `/`-separated path. This is summarized below.

```plaintext
latch://<DOMAIN><PATH>
```

In some CLI commands (notably [`latch cp`](../cli/cp.md) and [`latch mv`](../cli/mv.md)), the `latch` prefix can be omitted, resulting in a URL of the form `://<DOMAIN><PATH>`.

### Domains

A domain can be in one of several different forms. Each of these domains affect the way that the path following it is resolved.

- `<ACCOUNT_ID>.account`: Resolve the path as if it were a path in the specified account.
- `<BUCKET_NAME>.mount`: Resolve the path as if it were a path in the mounted S3 bucket specified. (Note: the bucket must be mounted to Latch first)
- `<NODE_ID>.node`: Resolve the path as if it were a relative path under the specified node.
- `shared.<ACCOUNT_ID>.account`: Resolve the path as if it were shared in the specified account.

In addition to these, the empty domain (paths that look like `latch:///...`) and the domain `shared` are both valid. When used, their behavior depends on the workspace that the user is currently in.

Specifically, `latch:///...` is treated the same as `latch://<CURRENT_WORKSPACE>.account/...`, and `latch://shared/...` is treated the same as `latch://shared.<CURRENT_WORKSPACE>.account/...`.

### Paths

The path following the domain, if provided, must be an absolute `/`-separated path. In `<NODE_ID>.node` domains, the path is resolved relative to the node specified. In `<BUCKET_NAME>.mount` domains, the path is resolved as if it were an S3 key in the mounted bucket.

A path can be omitted altogether (i.e. a path of the form `latch://<DOMAIN>`) if and only if the domain is of the form `<NODE_ID>.node`.

Whether or not a path ends with a slash does not affect the file or directory it resolves to. However, it may affect the result of a command that uses it (e.g. [`latch cp`](../cli/cp.md)).

## Examples

- `latch:///` points to root directory in the user's current workspace.
- `latch://71.account/bottomly/genomic.fna/` points to the file at `/bottomly/genomic.fna` in the account with id `71`.
- `latch://shared/results/summary.csv` points to the file at `/results/summary.csv`, that was shared to the user's current workspace.
- `latch://mount-test.mount/11211a11.fastq` points to the file with key `11211a11.fastq` in the S3 bucket `mount-test`.
- `latch://2698497.node` points to the node with ID `2698497`.
- `latch://2698497.node/file.txt` points to the child of the node with ID `2698497` called `file.txt`.
