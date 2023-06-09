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
#include "matchdoc/MatchDocFormatter.h"

namespace matchdoc {

using namespace std;
using namespace autil;

AUTIL_LOG_SETUP(matchdoc, MatchDocFormatter);

// 要求 fieldNameVec 和 refs 等长。caller最好在调用前排除掉(!ref->getValueType().isBuiltInType()) && (ref->meta.vt != typeid(std::string).name()) 的字段
flatbuffers::Offset<MatchRecords> MatchDocFormatter::toFBResultRecordsByColumns(
        const string &tableName,
        const vector<string> &fieldNameVec,
        const ReferenceVector &refs,
        const vector<MatchDoc> &docs,
        const vector<Offset<flatbuffers::String>> &tracerOffsets,
        flatbuffers::FlatBufferBuilder &fbb) {
    vector<flatbuffers::Offset<FieldValueColumnTable>> fbRecordColumnVec;
    flatbuffers::Offset<MatchRecords> fbMatchRecords;
    for (const auto &ref : refs) {
        flatbuffers::Offset<FieldValueColumnTable> fbRecordColumn =
            toFBResultRecordsByColumn(ref, docs, fbb);
        fbRecordColumnVec.push_back(fbRecordColumn);
    }

    size_t docCount = docs.size();

    if (!tracerOffsets.empty()) {
        return CreateMatchRecords(fbb,
                fbb.CreateVectorOfStrings(fieldNameVec),
                fbb.CreateVector(fbRecordColumnVec),
                docCount,
                fbb.CreateVector(tracerOffsets),
                fbb.CreateString(tableName));
    }

    return CreateMatchRecords(fbb,
                              fbb.CreateVectorOfStrings(fieldNameVec),
                              fbb.CreateVector(fbRecordColumnVec),
                              docCount, 0, fbb.CreateString(tableName));
}

flatbuffers::Offset<MatchRecords> MatchDocFormatter::toFBResultRecordsByColumns(
        const string &tableName,
        const vector<string> &fieldNameVec,
        const ReferenceVector &refs,
        const vector<MatchDoc> &docs,
        flatbuffers::FlatBufferBuilder &fbb,
        string &errMsg) {
    return toFBResultRecordsByColumns(tableName, fieldNameVec, refs, docs, {}, fbb, errMsg);
}

auto dealWithTrivial(
        const vector<string> &fieldNames,
        flatbuffers::FlatBufferBuilder &fbb,
        std::string &errMsg) {
    vector<flatbuffers::Offset<FieldValueColumnTable>> fbRecordColumnVec;
    errMsg = "";
    return CreateMatchRecords(fbb, fbb.CreateVectorOfStrings(fieldNames), fbb.CreateVector(fbRecordColumnVec));
}

flatbuffers::Offset<MatchRecords> MatchDocFormatter::toFBResultRecordsByColumns(
        const string &tableName,
        const vector<string> &fieldNameVec,
        const ReferenceVector &refs,
        const vector<MatchDoc> &docs,
        const vector<Offset<flatbuffers::String>> &tracerOffsets,
        flatbuffers::FlatBufferBuilder &fbb,
        string &errMsg) {

    if (docs.empty() || refs.empty()) {
        return dealWithTrivial(fieldNameVec, fbb, errMsg);
    }
    return toFBResultRecordsByColumns(tableName, fieldNameVec, refs, docs, tracerOffsets, fbb);
}

flatbuffers::Offset<FieldValueColumnTable> MatchDocFormatter::toFBResultRecordsByColumn(
        const matchdoc::ReferenceBase *base, const vector<MatchDoc> &docs, flatbuffers::FlatBufferBuilder &fbb) {
    uint32_t recordSize = docs.size();
    matchdoc::BuiltinType builtinType = bt_unknown;
    if (base->getValueType().isBuiltInType()) {
        builtinType = base->getValueType().getBuiltinType();
    }

    auto offsetDefault = CreateUnknownValueColumn(fbb, (int8_t)0).Union();
    flatbuffers::Offset<FieldValueColumnTable> defaultRet = CreateFieldValueColumnTable(fbb, FieldValueColumn_UnknownValueColumn, offsetDefault);
    bool isMulti = base->getValueType().isMultiValue();
    if (bt_unknown == builtinType) {  // VT_STRING的FB构造比较特殊
        if (typeid(string).name() != base->getVariableType()) {
            AUTIL_LOG(ERROR, "not support [%s] type fb format", base->getVariableType().c_str());
            return defaultRet;
        }
        auto ref = static_cast<const Reference<string> *>(base);
        std::vector<flatbuffers::Offset<flatbuffers::String>> offsets(recordSize);
        for (size_t i = 0; i < recordSize; i++) {
            string value = ref->get(docs[i]);
            offsets[i] = fbb.CreateString(value.data(), value.size());
        }
        flatbuffers::Offset<StringValueColumn> fbValue =
            CreateStringValueColumn(fbb, fbb.CreateVector(offsets));
        flatbuffers::Offset<void> field_value_column = fbValue.Union();
        return CreateFieldValueColumnTable(fbb, FieldValueColumn_StringValueColumn,
                field_value_column);
    } else if (bt_string == builtinType) {
        if (false == isMulti) {
            auto isStdType = base->getValueType().isStdType();
            std::vector<flatbuffers::Offset<flatbuffers::String>> offsets(recordSize);
            for (size_t i = 0; i < recordSize; i++) {
                if (isStdType) {
                    auto ref = static_cast<const Reference<std::string> *>(base);
                    std::string value = ref->get(docs[i]);
                    offsets[i] = fbb.CreateString(value.data(), value.size());
                }
                else {
                    auto ref = static_cast<const Reference<MultiChar> *>(base);
                    MultiChar value = ref->get(docs[i]);
                    offsets[i] = fbb.CreateString(value.data(), value.size());
                }
            }
            flatbuffers::Offset<StringValueColumn> fbValue =
                CreateStringValueColumn(fbb, fbb.CreateVector(offsets));
            flatbuffers::Offset<void> field_value_column = fbValue.Union();
            return CreateFieldValueColumnTable(fbb, FieldValueColumn_StringValueColumn,
                    field_value_column);
        } else {
            auto ref = static_cast<const Reference<autil::MultiString> *>(base);
            if (NULL == ref) {
                AUTIL_LOG(WARN, "MultiString ref is null");
                return defaultRet;
            }
            vector<flatbuffers::Offset<MultiStringValue>> offsetVec(recordSize);
            for (size_t i = 0; i < recordSize; ++i) {
                autil::MultiString valueList = ref->get(docs[i]);
                uint32_t value_size = valueList.size();
                std::vector<flatbuffers::Offset<flatbuffers::String>> offsets(value_size);
                for (size_t j = 0; j < valueList.size(); ++j) {
                    offsets[j] = fbb.CreateString(valueList[j].data(), valueList[j].size());
                }
                flatbuffers::Offset<MultiStringValue> offset =
                    CreateMultiStringValue(fbb, fbb.CreateVector(offsets));
                offsetVec[i] = offset;
            }
            flatbuffers::Offset<MultiStringValueColumn> fbValue =
                CreateMultiStringValueColumn(fbb, fbb.CreateVector(offsetVec));
            flatbuffers::Offset<void> fieldValueColumn = fbValue.Union();
            return CreateFieldValueColumnTable(fbb, FieldValueColumn_MultiStringValueColumn,
                    fieldValueColumn);
        }
    } else {
#define DECLARE_FIELD_HELPER(VT, fb_field_column_type, create_function, fb_field_column_enum_type, one_field_type,         \
                             multi_create_one_field_function, multi_fb_field_column_type, multi_create_function,           \
                             multi_fb_field_column_enum_type)                                                              \
        case VT: {                                                                                                         \
            if (isMulti) {                                                                                                 \
                typedef matchdoc::MatchDocBuiltinType2CppType<VT, true>::CppType FieldType;                                \
                typedef matchdoc::MatchDocBuiltinType2CppType<VT, false>::CppType SingleFieldType;                         \
                auto ref = static_cast<const Reference<FieldType> *>(base);                                                \
                vector<flatbuffers::Offset<one_field_type>> offsetVec;                                                     \
                for (uint32_t i = 0; i < recordSize; ++i) {                                                                \
                    FieldType value = ref->get(docs[i]);                                                                   \
                    flatbuffers::Offset<one_field_type> offset = multi_create_one_field_function(                          \
                            fbb, fbb.CreateVector(static_cast<const SingleFieldType *>(value.data()), value.size()));      \
                    offsetVec.push_back(offset);                                                                           \
                }                                                                                                          \
                flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<one_field_type>>> offset =                     \
                            fbb.CreateVector(offsetVec);                                                                   \
                flatbuffers::Offset<multi_fb_field_column_type> valueColumnOffset =                                        \
                            multi_create_function(fbb, offset);                                                            \
                return CreateFieldValueColumnTable(fbb, multi_fb_field_column_enum_type, valueColumnOffset.Union());       \
            } else {                                                                                                       \
                typedef matchdoc::MatchDocBuiltinType2CppType<VT, false>::CppType FieldType;                               \
                auto ref = static_cast<const Reference<FieldType> *>(base);                                                \
                vector<FieldType> vec;                                                                                     \
                for (uint32_t i = 0; i < recordSize; ++i) {                                                                \
                    FieldType value = ref->get(docs[i]);                                                                   \
                    vec.push_back(value);                                                                                  \
                }                                                                                                          \
                flatbuffers::Offset<fb_field_column_type> fbValue = create_function(fbb, fbb.CreateVector(vec));           \
                return CreateFieldValueColumnTable(fbb, fb_field_column_enum_type, fbValue.Union());                       \
            }                                                                                                              \
            break;                                                                                                         \
        }
        switch (builtinType) {
            DECLARE_FIELD_HELPER(matchdoc::bt_int8, Int8ValueColumn, CreateInt8ValueColumn,
                    FieldValueColumn_Int8ValueColumn, MultiInt8Value, CreateMultiInt8Value,
                    MultiInt8ValueColumn, CreateMultiInt8ValueColumn,
                    FieldValueColumn_MultiInt8ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_int16, Int16ValueColumn, CreateInt16ValueColumn,
                        FieldValueColumn_Int16ValueColumn, MultiInt16Value, CreateMultiInt16Value,
                        MultiInt16ValueColumn, CreateMultiInt16ValueColumn,
                        FieldValueColumn_MultiInt16ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_int32, Int32ValueColumn, CreateInt32ValueColumn,
                        FieldValueColumn_Int32ValueColumn, MultiInt32Value, CreateMultiInt32Value,
                        MultiInt32ValueColumn, CreateMultiInt32ValueColumn,
                        FieldValueColumn_MultiInt32ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_int64, Int64ValueColumn, CreateInt64ValueColumn,
                        FieldValueColumn_Int64ValueColumn, MultiInt64Value, CreateMultiInt64Value,
                        MultiInt64ValueColumn, CreateMultiInt64ValueColumn,
                        FieldValueColumn_MultiInt64ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_uint8, UInt8ValueColumn, CreateUInt8ValueColumn,
                        FieldValueColumn_UInt8ValueColumn, MultiUInt8Value, CreateMultiUInt8Value,
                        MultiUInt8ValueColumn, CreateMultiUInt8ValueColumn,
                        FieldValueColumn_MultiUInt8ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_uint16, UInt16ValueColumn, CreateUInt16ValueColumn,
                        FieldValueColumn_UInt16ValueColumn, MultiUInt16Value, CreateMultiUInt16Value,
                        MultiUInt16ValueColumn, CreateMultiUInt16ValueColumn,
                        FieldValueColumn_MultiUInt16ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_uint32, UInt32ValueColumn, CreateUInt32ValueColumn,
                        FieldValueColumn_UInt32ValueColumn, MultiUInt32Value, CreateMultiUInt32Value,
                        MultiUInt32ValueColumn, CreateMultiUInt32ValueColumn,
                        FieldValueColumn_MultiUInt32ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_uint64, UInt64ValueColumn, CreateUInt64ValueColumn,
                        FieldValueColumn_UInt64ValueColumn, MultiUInt64Value, CreateMultiUInt64Value,
                        MultiUInt64ValueColumn, CreateMultiUInt64ValueColumn,
                        FieldValueColumn_MultiUInt64ValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_float, FloatValueColumn, CreateFloatValueColumn,
                        FieldValueColumn_FloatValueColumn, MultiFloatValue, CreateMultiFloatValue,
                        MultiFloatValueColumn, CreateMultiFloatValueColumn,
                        FieldValueColumn_MultiFloatValueColumn)
                DECLARE_FIELD_HELPER(matchdoc::bt_double, DoubleValueColumn, CreateDoubleValueColumn,
                        FieldValueColumn_DoubleValueColumn, MultiDoubleValue, CreateMultiDoubleValue,
                        MultiDoubleValueColumn, CreateMultiDoubleValueColumn,
                        FieldValueColumn_MultiDoubleValueColumn)
        default:
                break;
        }
#undef DECLARE_FIELD_HELPER
    }

    return defaultRet;
}

flatbuffers::Offset<MatchRecords> MatchDocFormatter::createMatchRecords(
        flatbuffers::FlatBufferBuilder &fbb,
        const vector<string> &fieldNames,
        const vector<vector<int8_t>> &int8Values,
        const vector<vector<uint8_t>> &uint8Values,
        const vector<vector<int16_t>> &int16Values,
        const vector<vector<uint16_t>> &uint16Values,
        const vector<vector<int32_t>> &int32Values,
        const vector<vector<uint32_t>> &uint32Values,
        const vector<vector<int64_t>> &int64Values,
        const vector<vector<uint64_t>> &uint64Values,
        const vector<vector<float>> &floatValues,
        const vector<vector<double>> &doubleValues,
        const vector<vector<string>> &stringValues,
        const vector<vector<vector<int8_t>>> &multiInt8Values,
        const vector<vector<vector<uint8_t>>> &multiUint8Values,
        const vector<vector<vector<int16_t>>> &multiInt16Values,
        const vector<vector<vector<uint16_t>>> &multiUint16Values,
        const vector<vector<vector<int32_t>>> &multiInt32Values,
        const vector<vector<vector<uint32_t>>> &multiUint32Values,
        const vector<vector<vector<int64_t>>> &multiInt64Values,
        const vector<vector<vector<uint64_t>>> &multiUint64Values,
        const vector<vector<vector<float>>> &multiFloatValues,
        const vector<vector<vector<double>>> &multiDoubleValues,
        const vector<vector<vector<string>>> &multiStringValues,
        size_t docCount,
        const vector<string> &tracers) {
    vector<Offset<FieldValueColumnTable>> fieldVec;

#define PUSH_SINGLE_VALUES(VecValues, ValueColumn)                      \
    for (const auto &values: VecValues) {                               \
        fieldVec.emplace_back(CreateFieldValueColumnTable(              \
                        fbb,                                            \
                        FieldValueColumn_##ValueColumn,                 \
                        Create##ValueColumn(fbb,fbb.CreateVector(values)).Union())); \
    }

#define PUSH_SINGLE_STRINGS(VecValues, ValueColumn)                     \
    for (const auto &values: VecValues) {                               \
        fieldVec.emplace_back(CreateFieldValueColumnTable(              \
                        fbb,                                            \
                        FieldValueColumn_##ValueColumn,                 \
                        Create##ValueColumn(fbb, fbb.CreateVectorOfStrings(values)).Union())); \
    }

    PUSH_SINGLE_VALUES(int8Values, Int8ValueColumn);
    PUSH_SINGLE_VALUES(uint8Values, UInt8ValueColumn);
    PUSH_SINGLE_VALUES(int16Values, Int16ValueColumn);
    PUSH_SINGLE_VALUES(uint16Values, UInt16ValueColumn);
    PUSH_SINGLE_VALUES(int32Values, Int32ValueColumn);
    PUSH_SINGLE_VALUES(uint32Values, UInt32ValueColumn);
    PUSH_SINGLE_VALUES(int64Values, Int64ValueColumn);
    PUSH_SINGLE_VALUES(uint64Values, UInt64ValueColumn);
    PUSH_SINGLE_VALUES(floatValues, FloatValueColumn);
    PUSH_SINGLE_VALUES(doubleValues, DoubleValueColumn);
    PUSH_SINGLE_STRINGS(stringValues, StringValueColumn);

#define PUSH_MULTI_VALUES(VecValues, MultiValue)                        \
    for (const auto &values: VecValues) {                               \
        std::vector<Offset<MultiValue>> offsetVec;                      \
        for (const auto &value: values) {                               \
            offsetVec.emplace_back(                                     \
                    Create##MultiValue(fbb, fbb.CreateVector(value)));  \
        }                                                               \
        fieldVec.emplace_back(                                          \
                CreateFieldValueColumnTable(fbb,                        \
                        FieldValueColumn_##MultiValue##Column,          \
                        Create##MultiValue##Column(fbb,fbb.CreateVector(offsetVec)).Union() ) ) ; \
}

#define PUSH_MULTI_STRINGS(VecValues, MultiValue)                       \
    for (const auto &values: VecValues) {                               \
        std::vector<Offset<MultiValue>> offsetVec;                      \
        for (const auto &value: values) {                               \
            offsetVec.emplace_back(Create##MultiValue(fbb,              \
                            fbb.CreateVectorOfStrings(value)));         \
        }                                                               \
        fieldVec.emplace_back(                                          \
                CreateFieldValueColumnTable(fbb,                        \
                        FieldValueColumn_##MultiValue##Column,          \
                        Create##MultiValue##Column(fbb,                 \
                                fbb.CreateVector(offsetVec)).Union())); \
}


    PUSH_MULTI_VALUES(multiInt8Values, MultiInt8Value);
    PUSH_MULTI_VALUES(multiUint8Values, MultiUInt8Value);
    PUSH_MULTI_VALUES(multiInt16Values, MultiInt16Value);
    PUSH_MULTI_VALUES(multiUint16Values, MultiUInt16Value);
    PUSH_MULTI_VALUES(multiInt32Values, MultiInt32Value);
    PUSH_MULTI_VALUES(multiUint32Values, MultiUInt32Value);
    PUSH_MULTI_VALUES(multiInt64Values, MultiInt64Value);
    PUSH_MULTI_VALUES(multiUint64Values, MultiUInt64Value);
    PUSH_MULTI_VALUES(multiFloatValues, MultiFloatValue);
    PUSH_MULTI_VALUES(multiDoubleValues, MultiDoubleValue);
    PUSH_MULTI_STRINGS(multiStringValues, MultiStringValue);

#undef PUSH_SINGLE_VALUES
#undef PUSH_MULTI_VALUES


    return CreateMatchRecords(fbb,
                              fbb.CreateVectorOfStrings(fieldNames),
                              fbb.CreateVector(fieldVec),
                              docCount,
                              fbb.CreateVectorOfStrings(tracers)
                              );
}



}
