from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union

import flyteidl.core.types_pb2 as pb
import google.protobuf.struct_pb2 as pb_struct

from ..utils import merged_pb, to_idl_many, try_to_idl


class SimpleType(int, Enum):
    """Define a set of simple types."""

    none = pb.NONE
    integer = pb.INTEGER
    float = pb.FLOAT
    string = pb.STRING
    boolean = pb.BOOLEAN
    datetime = pb.DATETIME
    duration = pb.DURATION
    binary = pb.BINARY
    error = pb.ERROR
    struct = pb.STRUCT

    def to_idl(self) -> pb.SimpleType:
        return self.value


@dataclass
class SchemaType:
    """Defines schema columns and types to strongly type-validate schemas interoperability."""

    @dataclass
    class SchemaColumn:
        name: str
        """A unique name -within the schema type- for the column"""

        class SchemaColumnType(int, Enum):
            """Define a set of simple types."""

            integer = pb.SchemaType.SchemaColumn.INTEGER
            float = pb.SchemaType.SchemaColumn.FLOAT
            string = pb.SchemaType.SchemaColumn.STRING
            boolean = pb.SchemaType.SchemaColumn.BOOLEAN
            datetime = pb.SchemaType.SchemaColumn.DATETIME
            duration = pb.SchemaType.SchemaColumn.DURATION

            def to_idl(self) -> pb.SchemaType.SchemaColumn.SchemaColumnType:
                return self.value

        type: SchemaColumnType
        """The column type. This allows a limited set of types currently."""

        def to_idl(self) -> pb.SchemaType.SchemaColumn:
            return pb.SchemaType.SchemaColumn(name=self.name, type=self.type.to_idl())

    columns: Iterable[SchemaColumn]
    """A list of ordered columns this schema comprises of."""

    def to_idl(self) -> pb.SchemaType:
        return pb.SchemaType(columns=to_idl_many(self.columns))


@dataclass
class StructuredDatasetType:
    @dataclass
    class DatasetColumn:
        name: str
        """A unique name within the schema type for the column."""

        literal_type: "LiteralType"
        """The column type."""

        def to_idl(self) -> pb.StructuredDatasetType.DatasetColumn:
            return pb.StructuredDatasetType.DatasetColumn(
                name=self.name, literal_type=self.literal_type.to_idl()
            )

    columns: Iterable[DatasetColumn]
    """A list of ordered columns this schema comprises of."""

    format: str
    """
    This is the storage format, the format of the bits at rest
    parquet, feather, csv, etc.
    For two types to be compatible, the format will need to be an exact match.
    """

    external_schema_type: Optional[str] = None
    """
    This is a string representing the type that the bytes in external_schema_bytes are formatted in.
    This is an optional field that will not be used for type checking.
    """

    external_schema_bytes: Optional[bytes] = None
    """
    The serialized bytes of a third-party schema library like Arrow.
    This is an optional field that will not be used for type checking.
    """

    def to_idl(self) -> pb.StructuredDatasetType:
        return pb.StructuredDatasetType(
            columns=to_idl_many(self.columns),
            format=self.format,
            external_schema_type=self.external_schema_type,
            external_schema_bytes=self.external_schema_bytes,
        )


@dataclass
class BlobType:
    """Defines type behavior for blob objects"""

    class BlobDimensionality(int, Enum):
        """Define a set of simple types."""

        single = pb.BlobType.SINGLE
        multipart = pb.BlobType.MULTIPART

        def to_idl(self) -> pb.BlobType.BlobDimensionality:
            return self.value

    dimensionality: BlobDimensionality

    format: str = ""
    """
    Format can be a free form string understood by SDK/UI etc like
    csv, parquet etc
    """

    def to_idl(self) -> pb.BlobType:
        return pb.BlobType(
            format=self.format, dimensionality=self.dimensionality.to_idl()
        )


@dataclass
class EnumType:
    """
    Enables declaring enum types, with predefined string values
    For len(values) > 0, the first value in the ordered list is regarded as the default value. If you wish
    To provide no defaults, make the first value as undefined.
    """

    values: Iterable[str]
    """Predefined set of enum values."""

    def to_idl(self) -> pb.EnumType:
        return pb.EnumType(values=self.values)


@dataclass
class UnionType:
    """
    Defines a tagged union type, also known as a variant (and formally as the sum type).

    A sum type S is defined by a sequence of types (A, B, C, ...), each tagged by a string tag
    A value of type S is constructed from a value of any of the variant types. The specific choice of type is recorded by
    storing the varaint's tag with the literal value and can be examined in runtime.

    Type S is typically written as
    S := Apple A | Banana B | Cantaloupe C | ...

    Notably, a nullable (optional) type is a sum type between some type X and the singleton type representing a null-value:
    Optional X := X | Null

    See also: https://en.wikipedia.org/wiki/Tagged_union
    """

    variants: "Iterable[LiteralType]"
    """Predefined set of variants in union."""

    def to_idl(self) -> pb.UnionType:
        return pb.UnionType(variants=to_idl_many(self.variants))


@dataclass
class RecordFieldType:
    key: str
    type: "LiteralType"

    def to_idl(self) -> pb.RecordFieldType:
        return pb.RecordFieldType(key=self.key, type=self.type.to_idl())


@dataclass
class RecordType:
    fields: Iterable[RecordFieldType]

    def to_idl(self) -> pb.RecordType:
        return pb.RecordType(fields=to_idl_many(self.fields))


@dataclass
class TypeStructure:
    """
    Hints to improve type matching
    e.g. allows distinguishing output from custom type transformers
    even if the underlying IDL serialization matches.
    """

    tag: str
    """Must exactly match for types to be castable"""

    def to_idl(self) -> pb.TypeStructure:
        return pb.TypeStructure(tag=self.tag)


@dataclass
class TypeAnnotation:
    """TypeAnnotation encapsulates registration time information about a type. This can be used for various control-plane operations. TypeAnnotation will not be available at runtime when a task runs."""

    annotations: pb_struct.Struct
    """A arbitrary JSON payload to describe a type."""

    def to_idl(self) -> pb.TypeAnnotation:
        return pb.TypeAnnotation(annotations=self.annotations)


@dataclass
class LiteralType:
    """Defines a strong type to allow type checking between interfaces."""

    type: "Union[LiteralTypeSimple, LiteralTypeSchema, LiteralTypeCollection, LiteralTypeMap, LiteralTypeBlob, LiteralTypeEnum, LiteralTypeStructuredDataset, LiteralTypeUnion, LiteralTypeRecord]"

    metadata: Optional[pb_struct.Struct] = None
    """
    This field contains type metadata that is descriptive of the type, but is NOT considered in type-checking. This might be used by
    consumers to identify special behavior or display extended information for the type.

    maximsmol: note: old-style dataclass serialization used metadata when type-checking
    though the original comment really refers to how propeller treats the type
    and iirc propeller always ignores .metadata
    """

    annotation: Optional[TypeAnnotation] = None
    """
    This field contains arbitrary data that might have special semantic
    meaning for the client but does not effect internal flyte behavior.
    """

    structure: Optional[TypeStructure] = None
    """Hints to improve type matching."""

    def to_idl(self) -> pb.LiteralType:
        return merged_pb(
            pb.LiteralType(
                metadata=self.metadata,
                annotation=try_to_idl(self.annotation),
                structure=try_to_idl(self.structure),
            ),
            self.type,
        )


@dataclass
class LiteralTypeSimple:
    """A simple type that can be compared one-to-one with another."""

    simple: SimpleType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(simple=self.simple.to_idl())


@dataclass
class LiteralTypeSchema:
    """A complex type that requires matching of inner fields."""

    schema: SchemaType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(schema=self.schema.to_idl())


@dataclass
class LiteralTypeCollection:
    """Defines the type of the value of a collection. Only homogeneous collections are allowed."""

    collection_type: LiteralType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(collection_type=self.collection_type.to_idl())


@dataclass
class LiteralTypeMap:
    """Defines the type of the value of a map type. The type of the key is always a string."""

    map_value_type: LiteralType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(map_value_type=self.map_value_type.to_idl())


@dataclass
class LiteralTypeBlob:
    """A blob might have specialized implementation details depending on associated metadata."""

    blob: BlobType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(blob=self.blob.to_idl())


@dataclass
class LiteralTypeEnum:
    """Defines an enum with pre-defined string values."""

    enum_type: EnumType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(enum_type=self.enum_type.to_idl())


@dataclass
class LiteralTypeStructuredDataset:
    """Generalized schema support"""

    structured_dataset_type: StructuredDatasetType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(
            structured_dataset_type=self.structured_dataset_type.to_idl()
        )


@dataclass
class LiteralTypeUnion:
    """Defines an union type with pre-defined LiteralTypes."""

    union_type: UnionType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(union_type=self.union_type.to_idl())


@dataclass
class LiteralTypeRecord:
    record_type: RecordType

    def to_idl(self) -> pb.LiteralType:
        return pb.LiteralType(record_type=self.record_type.to_idl())


@dataclass
class OutputReference:
    """
    A reference to an output produced by a node. The type can be retrieved -and validated- from
    the underlying interface of the node.
    """

    node_id: str
    """Node id must exist at the graph layer."""

    var: str
    """Variable name must refer to an output variable for the node."""

    def to_idl(self) -> pb.OutputReference:
        return pb.OutputReference(node_id=self.node_id, var=self.var)


@dataclass
class Error:
    """Represents an error thrown from a node."""

    failed_node_id: str
    """The node id that threw the error."""
    message: str
    """Error message thrown."""

    def to_idl(self) -> pb.Error:
        return pb.Error(failed_node_id=self.failed_node_id, message=self.message)
