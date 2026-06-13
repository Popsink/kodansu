# object_store (S3): `DeleteObjects` 200-response with a top-level `<Error>` (SlowDown) body fails deserialization instead of being retried

**Crate:** `object_store` 0.13.2 (latest published), feature `aws`

## What happens

S3 can throttle a multi-object `DeleteObjects` (`POST /?delete`) by returning **HTTP 200** whose body is a top-level error document:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message><RequestId>...</RequestId><HostId>...</HostId></Error>
```

rather than the expected `<DeleteResult>` with per-key `<Deleted>`/`<Error>` entries.

In `AmazonS3Client::bulk_delete_request` (`src/aws/client.rs`):

- `send_retry(&self.config.retry_config)` retries only on the HTTP **status code**, so a `200` is treated as success and never retried;
- the body is then fed to `quick_xml::de::from_reader::<BatchDeleteResponse>`, whose `content` is `Vec<DeleteObjectResult>` with `#[serde(rename_all = "PascalCase")] enum { Deleted(..), Error(..) }`. The top-level `<Code>` element is an unknown variant, so deserialization fails with:

```
unknown variant `Code`, expected `Deleted` or `Error`
```

surfaced as `Error::InvalidDeleteObjectsResponse`, a non-retryable error.

## Impact

A request-rate throttle on `DeleteObjects` (HTTP 200 + `<Error>SlowDown`) is reported as an opaque, **non-retryable** parse error even though **no objects were deleted** and a plain retry-after-backoff would have succeeded. Callers doing retention/cleanup deletes drop the delete entirely.

This is the same class of behaviour as [aws/aws-sdk-go#3707](https://github.com/aws/aws-sdk-go/issues/3707) ("S3 DeleteObjects fails silently on retry after 503"): for multi-object delete, S3 may convey a request-level error in the body of a 200.

## Suggested fix

In `bulk_delete_request`, before parsing as `<DeleteResult>`, detect a top-level `<Error>` document in the response body and:

- if its `<Code>` is a retryable throttle (`SlowDown`, `ServiceUnavailable`, `InternalError`, `503 SlowDown`), surface it as a **retryable** error so `send_retry`'s backoff applies (or retry inline); otherwise
- surface it as the corresponding typed S3 error rather than a generic deserialization failure.

A minimal version: attempt to deserialize the body as `BatchDeleteResponse`, and on failure attempt to deserialize as a top-level `<Error>` and map a `SlowDown`/5xx code to a retryable error.

## Environment

- `object_store` 0.13.2, `aws` feature, AWS S3 `eu-west-3`, high multi-object-delete throughput on a single key prefix (per-prefix rate limit, ~3,500 mutating req/s).
- Reproduces under sustained `DeleteObjects` load concentrated on one prefix.
