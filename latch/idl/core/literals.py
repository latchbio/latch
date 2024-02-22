import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime as datetime_
from datetime import timedelta
from typing import Optional

import flyteidl.core.literals_pb2 as pb
import google.protobuf.struct_pb2 as pb_struct
import google.protobuf.timestamp_pb2 as pb_ts

from ..utils import (
    dur_from_td,
    merged_pb,
    timestamp_from_datetime,
    to_idl_many,
    to_idl_mapping,
    try_to_idl,
)
from .types import (
    BlobType,
    Error,
    LiteralType,
    OutputReference,
    SchemaType,
    StructuredDatasetType,
)


@dataclass
class Primitive:
    """Primitive Types"""

    value: "typing.Union[PrimitiveInt, PrimitiveFloat, PrimitiveString, PrimitiveBoolean, PrimitiveDatetime, PrimitiveDuration]"
    """
    Defines one of simple primitive types. These types will get translated into different programming languages as
    described in https://developers.google.com/protocol-buffers/docs/proto#scalar.
    """

    def to_idl(self) -> pb.Primitive:
        return self.value.to_idl()


@dataclass
class PrimitiveInt:
    integer: int

    def to_idl(self) -> pb.Primitive:
        return pb.Primitive(integer=self.integer)


@dataclass
class PrimitiveFloat:
    float_value: float

    def to_idl(self) -> pb.Primitive:
        return pb.Primitive(float_value=self.float_value)


@dataclass
class PrimitiveString:
    string_value: str

    def to_idl(self) -> pb.Primitive:
        return pb.Primitive(string_value=self.string_value)


@dataclass
class PrimitiveBoolean:
    boolean: bool

    def to_idl(self) -> pb.Primitive:
        return pb.Primitive(boolean=self.boolean)


@dataclass
class PrimitiveDatetime:
    datetime: datetime_

    def to_idl(self) -> pb.Primitive:
        return pb.Primitive(datetime=timestamp_from_datetime(self.datetime))


@dataclass
class PrimitiveDuration:
    duration: timedelta

    def to_idl(self) -> pb.Primitive:
        return pb.Primitive(duration=dur_from_td(self.duration))


@dataclass
class Void:
    """
    Used to denote a nil/null/None assignment to a scalar value. The underlying LiteralType for Void is intentionally
    undefined since it can be assigned to a scalar of any LiteralType.

    maximsmol: note: Void can no longer be assigned to a scalar of any type since union types were introduced
    """

    def to_idl(self) -> pb.Void:
        return pb.Void()


@dataclass
class Blob:
    """
    Refers to an offloaded set of files. It encapsulates the type of the store and a unique uri for where the data is.
    There are no restrictions on how the uri is formatted since it will depend on how to interact with the store.
    """

    metadata: "BlobMetadata"
    uri: str

    def to_idl(self) -> pb.Blob:
        return pb.Blob(metadata=self.metadata.to_idl(), uri=self.uri)


@dataclass
class BlobMetadata:
    type: BlobType

    def to_idl(self) -> pb.BlobMetadata:
        return pb.BlobMetadata(type=self.type.to_idl())


@dataclass
class Binary:
    """
    A simple byte array with a tag to help different parts of the system communicate about what is in the byte array.
    It's strongly advisable that consumers of this type define a unique tag and validate the tag before parsing the data.
    """

    value: bytes
    tag: str

    def to_idl(self) -> pb.Binary:
        return pb.Binary(value=self.value, tag=self.tag)


@dataclass
class Schema:
    """
    A strongly typed schema that defines the interface of data retrieved from the underlying storage medium.

    maximsmol: note: pretty much unsupported
    """

    uri: str
    type: SchemaType

    def to_idl(self) -> pb.Schema:
        return pb.Schema(uri=self.uri, type=self.type.to_idl())


@dataclass
class Union:
    """The runtime representation of a tagged union value. See `UnionType` for more details."""

    value: "Literal"
    type: LiteralType

    def to_idl(self) -> pb.Union:
        return pb.Union(value=self.value.to_idl(), type=self.type.to_idl())


@dataclass
class RecordField:
    key: str
    value: "Literal"

    def to_idl(self) -> pb.RecordField:
        return pb.RecordField(key=self.key, value=self.value.to_idl())


@dataclass
class Record:
    fields: Iterable[RecordField]

    def to_idl(self) -> pb.Record:
        return pb.Record(fields=to_idl_many(self.fields))


@dataclass
class StructuredDatasetMetadata:
    """
    Bundle the type information along with the literal.
    This is here because StructuredDatasets can often be more defined at run time than at compile time.
    That is, at compile time you might only declare a task to return a pandas dataframe or a StructuredDataset,
    without any column information, but at run time, you might have that column information.
    flytekit python will copy this type information into the literal, from the type information, if not provided by
    the various plugins (encoders).
    Since this field is run time generated, it's not used for any type checking.
    """

    structured_dataset_type: StructuredDatasetType

    def to_idl(self) -> pb.StructuredDatasetMetadata:
        return pb.StructuredDatasetMetadata(
            structured_dataset_type=self.structured_dataset_type.to_idl()
        )


@dataclass
class StructuredDataset:
    uri: str
    """
    String location uniquely identifying where the data is.
    Should start with the storage location (e.g. s3://, gs://, bq://, etc.)
    """

    metadata: StructuredDatasetMetadata

    def to_idl(self) -> pb.StructuredDataset:
        return pb.StructuredDataset(uri=self.uri, metadata=self.metadata.to_idl())


@dataclass
class Scalar:
    value: "typing.Union[ScalarPrimitive, ScalarBlob, ScalarBinary, ScalarSchema, ScalarVoid, ScalarError, ScalarGeneric, ScalarStructuredDataset, ScalarUnion]"

    def to_idl(self) -> pb.Scalar:
        raise NotImplementedError()


@dataclass
class ScalarPrimitive:
    primitive: Primitive

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(primitive=self.primitive.to_idl())


@dataclass
class ScalarBlob:
    blob: Blob

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(blob=self.blob.to_idl())


@dataclass
class ScalarBinary:
    binary: Binary

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(binary=self.binary.to_idl())


@dataclass
class ScalarSchema:
    schema: Schema

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(schema=self.schema.to_idl())


@dataclass
class ScalarVoid:
    none_type: Void

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(none_type=self.none_type.to_idl())


@dataclass
class ScalarError:
    error: Error

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(error=self.error.to_idl())


@dataclass
class ScalarGeneric:
    """maximsmol: note: use Records i.e. dataclasses instead"""

    generic: pb_struct.Struct

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(generic=self.generic)


@dataclass
class ScalarStructuredDataset:
    structured_dataset: StructuredDataset

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(structured_dataset=self.structured_dataset.to_idl())


@dataclass
class ScalarUnion:
    union: Union

    def to_idl(self) -> pb.Scalar:
        return pb.Scalar(union=self.union.to_idl())


@dataclass
class Literal:
    value: "typing.Union[LiteralScalar, LiteralLiteralCollection, LiteralLiteralMap, LiteralRecord]"

    hash: Optional[str] = None
    """
    A hash representing this literal.
    This is used for caching purposes. For more details refer to RFC 1893
    (https://github.com/flyteorg/flyte/blob/516dd3926957af83c1c3ba6c12817477486be5c5/rfc/system/1893-caching-of-offloaded-objects.md)
    """

    def to_idl(self) -> pb.Literal:
        return merged_pb(pb.Literal(hash=self.hash), self.value)


@dataclass
class LiteralScalar:
    """A simple value."""

    scalar: Scalar

    def to_idl(self) -> pb.Literal:
        return pb.Literal(scalar=self.scalar.to_idl())


@dataclass
class LiteralLiteralCollection:
    """A collection of literals to allow nesting."""

    collection: "LiteralCollection"

    def to_idl(self) -> pb.Literal:
        return pb.Literal(collection=self.collection.to_idl())


@dataclass
class LiteralLiteralMap:
    """A map of strings to literals."""

    map: "LiteralMap"

    def to_idl(self) -> pb.Literal:
        return pb.Literal(map=self.map.to_idl())


@dataclass
class LiteralRecord:
    record: Record

    def to_idl(self) -> pb.Literal:
        return pb.Literal(record=self.record.to_idl())


@dataclass
class LiteralCollection:
    """A collection of literals. This is a workaround since oneofs in proto messages cannot contain a repeated field."""

    literals: Iterable[Literal]

    def to_idl(self) -> pb.LiteralCollection:
        return pb.LiteralCollection(literals=to_idl_many(self.literals))


@dataclass
class LiteralMap:
    """A map of literals. This is a workaround since oneofs in proto messages cannot contain a repeated field."""

    literals: Mapping[str, Literal]

    def to_idl(self) -> pb.LiteralMap:
        return pb.LiteralMap(literals=to_idl_mapping(self.literals))


@dataclass
class BindingDataCollection:
    """A collection of BindingData items."""

    bindings: "Iterable[BindingData]"

    def to_idl(self) -> pb.BindingDataCollection:
        return pb.BindingDataCollection(bindings=to_idl_many(self.bindings))


@dataclass
class BindingDataMap:
    """A map of BindingData items."""

    bindings: "Mapping[str, BindingData]"

    def to_idl(self) -> pb.BindingDataMap:
        return pb.BindingDataMap(bindings=to_idl_mapping(self.bindings))


@dataclass
class BindingDataRecordField:
    key: str
    binding: "BindingData"

    def to_idl(self) -> pb.BindingDataRecordField:
        return pb.BindingDataRecordField(key=self.key, binding=self.binding.to_idl())


@dataclass
class BindingDataRecord:
    fields: Iterable[BindingDataRecordField]

    def to_idl(self) -> pb.BindingDataRecord:
        return pb.BindingDataRecord(fields=to_idl_many(self.fields))


@dataclass
class UnionInfo:
    targetType: LiteralType

    def to_idl(self) -> pb.UnionInfo:
        return pb.UnionInfo(targetType=self.targetType.to_idl())


@dataclass
class BindingData:
    """Specifies either a simple value or a reference to another output."""

    value: "typing.Union[BindingDataScalar, BindingDataBindingCollection, BindingDataPromise, BindingDataBindingMap, BindingDataBindingRecord]"

    union: Optional[UnionInfo] = None

    def to_idl(self) -> pb.BindingData:
        return merged_pb(pb.BindingData(union=try_to_idl(self.union)), self.value)


@dataclass
class BindingDataScalar:
    """A simple scalar value."""

    scalar: Scalar

    def to_idl(self) -> pb.BindingData:
        return pb.BindingData(scalar=self.scalar.to_idl())


@dataclass
class BindingDataBindingCollection:
    """
    A collection of binding data. This allows nesting of binding data to any number
    of levels.
    """

    collection: BindingDataCollection

    def to_idl(self) -> pb.BindingData:
        return pb.BindingData(collection=self.collection.to_idl())


@dataclass
class BindingDataPromise:
    """References an output promised by another node."""

    promise: OutputReference

    def to_idl(self) -> pb.BindingData:
        return pb.BindingData(promise=self.promise.to_idl())


@dataclass
class BindingDataBindingMap:
    """A map of bindings. The key is always a string."""

    map: BindingDataMap

    def to_idl(self) -> pb.BindingData:
        return pb.BindingData(map=self.map.to_idl())


@dataclass
class BindingDataBindingRecord:
    """A map of bindings. The key is always a string."""

    record: BindingDataRecord

    def to_idl(self) -> pb.BindingData:
        return pb.BindingData(record=self.record.to_idl())


@dataclass
class Binding:
    """An input/output binding of a variable to either static value or a node output."""

    var: str
    """Variable name must match an input/output variable of the node."""
    binding: BindingData
    """Data to use to bind this variable."""

    def to_idl(self) -> pb.Binding:
        return pb.Binding(var=self.var, binding=self.binding.to_idl())


@dataclass
class KeyValuePair:
    """A generic key value pair."""

    key: str
    """required."""

    value: str
    """+optional."""

    def to_idl(self) -> pb.KeyValuePair:
        return pb.KeyValuePair(key=self.key, value=self.value)


@dataclass
class RetryStrategy:
    """Retry strategy associated with an executable unit."""

    retries: int
    """
    Number of retries. Retries will be consumed when the job fails with a recoverable error.
    The number of retries must be less than or equals to 10.
    """

    def to_idl(self) -> pb.RetryStrategy:
        return pb.RetryStrategy(retries=self.retries)
