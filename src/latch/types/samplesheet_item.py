from dataclasses import Field, dataclass, is_dataclass
from typing import ClassVar, Generic, Optional, Protocol, TypeVar, get_args, get_origin

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import (
    TypeEngine,
    TypeTransformer,
    TypeTransformerFailedError,
)
from flytekit.models.annotation import TypeAnnotation
from flytekit.models.literals import Literal, Record, RecordField
from flytekit.models.types import LiteralType, RecordFieldType, RecordType

from latch.registry.record import Record as RegistryRecord


# https://stackoverflow.com/questions/54668000/type-hint-for-an-instance-of-a-non-specific-dataclass
class _IsDataclass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field]]


T = TypeVar("T", bound=_IsDataclass)


@dataclass
class SamplesheetItem(Generic[T]):
    data: T

    # None if the row was manually input / pulled in from LData
    record: Optional[RegistryRecord]


class SamplesheetItemTypeTransformer(TypeTransformer[SamplesheetItem[T]]):
    def __init__(self):
        super().__init__(name="SamplesheetItem", t=SamplesheetItem)

    def assert_type(self, t: type[SamplesheetItem[T]], v: SamplesheetItem[T]):
        o = get_origin(t)
        a = get_args(t)

        if o is None or not issubclass(o, SamplesheetItem) or len(a) != 1:
            raise TypeTransformerFailedError(
                "Expected a `SamplesheetItem` with a single spec type"
            )

        if not is_dataclass(a[0]):
            raise TypeTransformerFailedError(
                "`SamplesheetItem` spec type must be a dataclass"
            )

        if not isinstance(v, SamplesheetItem) or not isinstance(v.data, a[0]):
            raise TypeTransformerFailedError("`SamplesheetItem`s are incompatible")

    def get_literal_type(self, t: type[SamplesheetItem[T]]) -> LiteralType:
        a = get_args(t)

        return LiteralType(
            record_type=RecordType([
                RecordFieldType("data", TypeEngine.to_literal_type(a[0])),
                RecordFieldType("record_id", TypeEngine.to_literal_type(Optional[str])),
            ]),
            annotation=TypeAnnotation({"samplesheet_item_with_record_id": True}),
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: SamplesheetItem[T],
        python_type: type[SamplesheetItem[T]],
        expected: LiteralType,
    ) -> Literal:
        if not isinstance(python_val, SamplesheetItem):
            raise TypeTransformerFailedError(
                "value provided was not a `SamplesheetItem`"
            )

        record_id: Optional[str] = None
        if python_val.record is not None:
            record_id = python_val.record.id

        a = get_args(python_type)

        return Literal(
            record=Record(
                fields=[
                    RecordField(
                        "data",
                        TypeEngine.to_literal(
                            ctx, python_val.data, a[0], TypeEngine.to_literal_type(a[0])
                        ),
                    ),
                    RecordField(
                        "record_id",
                        TypeEngine.to_literal(
                            ctx,
                            record_id,
                            Optional[str],
                            TypeEngine.to_literal_type(Optional[str]),
                        ),
                    ),
                ]
            )
        )

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: type[SamplesheetItem[T]],
    ) -> SamplesheetItem[T]:
        if lv.record is None:
            raise TypeTransformerFailedError("input `Literal` must be a `Record`")

        data_literal: Optional[Literal] = None
        record_literal: Optional[Literal] = None

        field: RecordField
        for field in lv.record.fields:
            if field.key == "data":
                data_literal = field.value
                continue

            if field.key == "record_id":
                record_literal = field.value
                continue

            raise TypeTransformerFailedError(
                f"invalid key in SamplesheetItem - {field.key}"
            )

        if data_literal is None or data_literal.record is None:
            raise TypeTransformerFailedError("SamplesheetItem missing 'data' key")
        if record_literal is None:
            raise TypeTransformerFailedError("SamplesheetItem missing 'record_id' key")

        data_type = get_args(expected_python_type)[0]
        data = TypeEngine.to_python_value(ctx, data_literal, data_type)

        record_id: Optional[str] = TypeEngine.to_python_value(
            ctx, record_literal, Optional[str]
        )

        record: Optional[RegistryRecord] = None
        if record_id is not None:
            record = RegistryRecord(id=record_id)

        return expected_python_type(data=data, record=record)


TypeEngine.register(SamplesheetItemTypeTransformer())
