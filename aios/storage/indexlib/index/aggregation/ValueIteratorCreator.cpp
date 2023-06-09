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
#include "indexlib/index/aggregation/ValueIteratorCreator.h"

#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/aggregation/AggregationReader.h"
#include "indexlib/index/aggregation/DistinctValueIterator.h"
#include "indexlib/index/aggregation/RangeIteratorCreator.h"
#include "indexlib/index/aggregation/SequentialMultiSegmentValueIterator.h"
#include "indexlib/index/aggregation/SortedMultiSegmentValueIterator.h"
#include "indexlib/index/aggregation/VersionMergingIteratorCreator.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"

namespace indexlibv2::index {

ValueIteratorCreator::ValueIteratorCreator(const AggregationReader* reader) : _reader(reader) {}

ValueIteratorCreator::~ValueIteratorCreator() = default;

ValueIteratorCreator& ValueIteratorCreator::FilterBy(const std::string& fieldName, const std::string& from,
                                                     const std::string& to)
{
    _range = std::make_unique<RangeParam>(RangeParam {fieldName, from, to});
    return *this;
}

ValueIteratorCreator& ValueIteratorCreator::Hint(IteratorHint hint)
{
    _hint = hint;
    return *this;
}

StatusOr<std::unique_ptr<IValueIterator>> ValueIteratorCreator::CreateIterator(uint64_t key,
                                                                               autil::mem_pool::Pool* pool) const
{
    auto leafIters = _reader->CreateLeafIters(key, pool);
    if (_range) {
        auto s = FilterByRange(leafIters);
        if (!s.IsOK()) {
            return {s.steal_error()};
        }
    }
    const auto& indexConfig = _reader->GetIndexConfig();
    bool needSort = IH_SORT == _hint || (IH_AUTO == _hint && indexConfig->IsDataSorted());
    if (!indexConfig->SupportDelete()) {
        return {CreateComposeIterator(std::move(leafIters), pool, needSort)};
    } else {
        bool flag = indexConfig->IsDataAndDeleteDataSameSort();
        auto data = CreateComposeIterator(std::move(leafIters), pool, needSort || flag);
        if (!data) {
            return {Status::InternalError("create data iterator failed")};
        }
        const auto& deleteReader = _reader->GetDeleteReader();
        assert(deleteReader);
        auto deleteLeafIters = deleteReader->CreateLeafIters(key, pool);
        auto deleteData = CreateComposeIterator(std::move(deleteLeafIters), pool, needSort || flag);
        if (!deleteData) {
            return {Status::InternalError("create delete iterator failed")};
        }
        if (!deleteData->HasNext()) {
            return {std::move(data)};
        }
        const auto& uniqueField = indexConfig->GetUniqueField();
        const auto& versionField = indexConfig->GetVersionField();
        const auto* packAttrFormatter = _reader->GetPackAttributeFormatter();
        const auto* delPackFmt = deleteReader->GetPackAttributeFormatter();
        VersionMergingIteratorCreator::DataIterator dataIter, delDataIter;
        dataIter.data = std::move(data);
        dataIter.uniqueFieldRef = packAttrFormatter->GetAttributeReference(uniqueField->GetFieldName());
        dataIter.versionFieldRef = packAttrFormatter->GetAttributeReference(versionField->GetFieldName());
        delDataIter.data = std::move(deleteData);
        delDataIter.uniqueFieldRef = delPackFmt->GetAttributeReference(uniqueField->GetFieldName());
        delDataIter.versionFieldRef = delPackFmt->GetAttributeReference(versionField->GetFieldName());
        auto sortPattern = !flag ? config::sp_nosort : indexConfig->GetSortDescriptions()[0].GetSortPattern();
        dataIter.sortPattern = sortPattern;
        delDataIter.sortPattern = sortPattern;
        return VersionMergingIteratorCreator::Create(std::move(dataIter), std::move(delDataIter), pool);
    }
}

StatusOr<std::unique_ptr<IValueIterator>>
ValueIteratorCreator::CreateDistinctIterator(uint64_t key, autil::mem_pool::Pool* pool,
                                             const std::string& fieldName) const
{
    const auto* packAttrFormatter = _reader->GetPackAttributeFormatter();
    AttributeReference* attrRef = packAttrFormatter->GetAttributeReference(fieldName);
    if (attrRef == nullptr) {
        return {Status::InternalError("distinct field %s does not exist in value", fieldName.c_str())};
    }
    const auto& indexConfig = _reader->GetIndexConfig();
    bool sorted = indexConfig->IsDataSortedBy(fieldName);
    if (sorted) {
        _hint = IH_SORT;
    }
    auto r = CreateIterator(key, pool);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    auto iter = std::move(r.steal_value());
#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        using T = typename indexlib::IndexlibFieldType2CppType<ft, false>::CppType;                                    \
        AttributeReferenceTyped<T>* typedRef = dynamic_cast<AttributeReferenceTyped<T>*>(attrRef);                     \
        if (sorted) {                                                                                                  \
            using IterType = DistinctSortedValueIterator<T>;                                                           \
            return {std::make_unique<IterType>(std::move(iter), typedRef)};                                            \
        } else {                                                                                                       \
            using IterType = DistinctUnsortedValueIterator<T>;                                                         \
            return {std::make_unique<IterType>(std::move(iter), typedRef)};                                            \
        }                                                                                                              \
    }
    switch (attrRef->GetFieldType()) {
        NUMERIC_FIELD_TYPE_MACRO_HELPER(CASE);
    default:
        return {Status::Unimplement("unsupported field type: %d", (int)attrRef->GetFieldType())};
    }
#undef CASE
}

Status ValueIteratorCreator::FilterByRange(std::vector<std::unique_ptr<IValueIterator>>& leafIters) const
{
    assert(_range);
    const auto* packAttrFormatter = _reader->GetPackAttributeFormatter();
    assert(packAttrFormatter);
    AttributeReference* ref = packAttrFormatter->GetAttributeReference(_range->fieldName);
    if (ref == nullptr) {
        return Status::InternalError("range field %s does not exist", _range->fieldName.c_str());
    }
    const auto& indexConfig = _reader->GetIndexConfig();
    bool dataSortedByRangeField = indexConfig->IsDataSortedBy(_range->fieldName);
    const auto& sortDescriptions = indexConfig->GetSortDescriptions();
    config::SortPattern sortOrder = sortDescriptions.empty() ? config::sp_nosort : sortDescriptions[0].GetSortPattern();
    RangeIteratorCreator creator(ref, dataSortedByRangeField, sortOrder);
    for (size_t i = 0; i < leafIters.size(); ++i) {
        auto r = creator.Create(std::move(leafIters[i]), _range->from, _range->to);
        if (!r.IsOK()) {
            return r.steal_error();
        }
        leafIters[i] = std::move(r.steal_value());
    }
    return Status::OK();
}

std::unique_ptr<IValueIterator>
ValueIteratorCreator::CreateComposeIterator(std::vector<std::unique_ptr<IValueIterator>> leafIters,
                                            autil::mem_pool::Pool* pool, bool needSort) const
{
    if (needSort) {
        return SortedMultiSegmentValueIterator::Create(std::move(leafIters), pool, _reader->GetDataComparator());
    } else {
        return SequentialMultiSegmentValueIterator::Create(std::move(leafIters));
    }
}

} // namespace indexlibv2::index
