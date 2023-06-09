/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "matchdoc/Trait.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"

namespace matchdoc {

template <FieldValueColumn ColumnType>
struct TraitTypeFromMatchDocColumn {};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_Int8ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = Int8ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_UInt8ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = UInt8ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_Int16ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = Int16ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_UInt16ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = UInt16ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_Int32ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = Int32ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_UInt32ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = UInt32ValueColumn;
};


template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_Int64ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = Int64ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_UInt64ValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = UInt64ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_FloatValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = FloatValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_DoubleValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = DoubleValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_StringValueColumn> {
    static constexpr bool isSingleValue() {
        return true;
    }
    using T = StringValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiInt8ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiInt8ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiUInt8ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiUInt8ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiInt16ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiInt16ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiUInt16ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiUInt16ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiInt32ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiInt32ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiUInt32ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiUInt32ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiInt64ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiInt64ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiUInt64ValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiUInt64ValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiFloatValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiFloatValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiDoubleValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiDoubleValueColumn;
};

template<>
struct TraitTypeFromMatchDocColumn<FieldValueColumn_MultiStringValueColumn> {
    static constexpr bool isSingleValue() {
        return false;
    }
    using T = MultiStringValueColumn;
};

}
