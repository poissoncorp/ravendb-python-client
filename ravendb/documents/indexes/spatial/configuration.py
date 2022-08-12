from __future__ import annotations
from enum import Enum
from typing import  Union, List, Dict


class SpatialFieldType(Enum):
    GEOGRAPHY = "Geography"
    CARTESIAN = "Cartesian"

    def __str__(self):
        return self.value


class SpatialRelation(Enum):
    WITHIN = "Within"
    CONTAINS = "Contains"
    DISJOINT = "Disjoint"
    INTERSECTS = "Intersects"


class SpatialSearchStrategy(Enum):
    GEOHASH_PREFIX_TREE = "GeohashPrefixTree"
    QUAD_PREFIX_TREE = "QuadPrefixTree"
    BOUNDING_BOX = "BoundingBox"

    def __str__(self):
        return self.value


class SpatialUnits:
    KILOMETERS = "Kilometers"
    MILES = "Miles"


class SpatialOptions:
    DEFAULT_GEOHASH_LEVEL = -9
    DEFAULT_QUAD_TREE_LEVEL = 23

    def __init__(
            self,
            field_type: SpatialFieldType = SpatialFieldType.GEOGRAPHY,
            strategy: SpatialSearchStrategy = SpatialSearchStrategy.GEOHASH_PREFIX_TREE,
            max_tree_level: int = DEFAULT_GEOHASH_LEVEL,
            min_x: int = -180,
            max_x: int = 180,
            min_y: int = -90,
            max_y: int = 90,
            units: SpatialUnits = SpatialUnits.KILOMETERS,
    ):
        self.type = field_type
        self.strategy = strategy
        self.max_tree_level = max_tree_level
        self.min_x = min_x
        self.max_x = max_x
        self.min_y = min_y
        self.max_y = max_y
        self.units = units

    @classmethod
    def from_copy(cls, options: SpatialOptions) -> SpatialOptions:
        return cls(
            options.type,
            options.strategy,
            options.max_tree_level,
            options.min_x,
            options.max_x,
            options.min_y,
            options.max_y,
            options.units,
        )

    def __eq__(self, other):
        if self == other:
            return True
        if other is None:
            return False
        if type(self) != type(other):
            return False

        other: SpatialOptions

        result = self.type == other.type and self.strategy == other.strategy
        if self.type == SpatialFieldType.GEOGRAPHY:
            result = result and self.units == other.units
        if self.strategy != SpatialSearchStrategy.BOUNDING_BOX:
            result = result and self.max_tree_level == other.max_tree_level
            if self.type == SpatialFieldType.CARTESIAN:
                result = (
                        result
                        and self.min_x == other.min_x
                        and self.max_x == other.max_x
                        and self.min_y == other.min_y
                        and self.max_y == other.max_y
                )
        return result

    # todo: hashCode() override


class AutoSpatialMethodType(Enum):
    POINT = "Point"
    WKT = "Wkt"

    def __str__(self):
        return self.value


class AutoSpatialOptions(SpatialOptions):
    def __init__(
            self,
            field_type: SpatialFieldType = SpatialFieldType.GEOGRAPHY,
            strategy: SpatialSearchStrategy = SpatialSearchStrategy.GEOHASH_PREFIX_TREE,
            max_tree_level: int = SpatialOptions.DEFAULT_GEOHASH_LEVEL,
            min_x: int = -180,
            max_x: int = 180,
            min_y: int = -90,
            max_y: int = 90,
            units: SpatialUnits = SpatialUnits.KILOMETERS,
    ):
        super(AutoSpatialOptions, self).__init__(
            field_type, strategy, max_tree_level, min_x, max_x, min_y, max_y, units
        )
        self.method_type: Union[None, AutoSpatialMethodType] = None
        self.method_argument: Union[None, List[str]] = None

    @staticmethod
    def from_json(json_dict: Dict) -> AutoSpatialOptions:
        return AutoSpatialOptions.from_copy(SpatialOptions.from_copy(json_dict.get("Options")))
