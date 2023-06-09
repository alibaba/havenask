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
#include "indexlib/index/aggregation/RangeIteratorCreator.h"

#include <algorithm>

#include "autil/StringUtil.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/aggregation/Filter.h"
#include "indexlib/index/aggregation/RandomAccessValueIterator.h"
#include "indexlib/index/aggregation/ValueIteratorWithFilter.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"

namespace indexlibv2::index {

RangeIteratorCreator::RangeIteratorCreator(const AttributeReference* rangeFieldRef, bool dataSortedByRangeField,
                                           config::SortPattern sortOrder)
    : _rangeFieldRef(rangeFieldRef)
    , _dataSortedByRangeField(dataSortedByRangeField)
    , _sortOrder(sortOrder)
{
}

StatusOr<std::unique_ptr<IValueIterator>> RangeIteratorCreator::Create(std::unique_ptr<IValueIterator> raw,
                                                                       const std::string& fromStr,
                                                                       const std::string& toStr) const
{
    if (fromStr.empty() && toStr.empty()) {
        return {std::move(raw)};
    }
    if (_rangeFieldRef->IsMultiValue() || !indexlib::IsNumericField(_rangeFieldRef->GetFieldType())) {
        return {Status::InternalError("filter field %s is not an single numeric field",
                                      _rangeFieldRef->GetAttrName().c_str())};
    }
#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        using T = typename indexlib::IndexlibFieldType2CppType<ft, false>::CppType;                                    \
        return CreateTyped<T>(std::move(raw), fromStr, toStr);                                                         \
    }
    switch (_rangeFieldRef->GetFieldType()) {
        NUMERIC_FIELD_TYPE_MACRO_HELPER(CASE);
    default:
        return {Status::InternalError("unsupported field type: %d", (int)_rangeFieldRef->GetFieldType())};
    }
#undef CASE
}

template <typename T>
StatusOr<std::unique_ptr<IValueIterator>> RangeIteratorCreator::CreateTyped(std::unique_ptr<IValueIterator> raw,
                                                                            const std::string& fromStr,
                                                                            const std::string& toStr) const
{
    auto* randomAccessIter = dynamic_cast<RandomAccessValueIterator*>(raw.get());
    if (randomAccessIter != nullptr && _dataSortedByRangeField) {
        return CreateRangeIterator<T>(std::move(raw), fromStr, toStr);
    } else {
        return CreateFilterIterator<T>(std::move(raw), fromStr, toStr);
    }
}

template <typename T>
StatusOr<std::unique_ptr<IValueIterator>> RangeIteratorCreator::CreateRangeIterator(std::unique_ptr<IValueIterator> raw,
                                                                                    const std::string& fromStr,
                                                                                    const std::string& toStr) const
{
    std::unique_ptr<RandomAccessValueIterator> randomIter(dynamic_cast<RandomAccessValueIterator*>(raw.release()));
    assert(randomIter);
    auto* typedRef = dynamic_cast<const AttributeReferenceTyped<T>*>(_rangeFieldRef);
    assert(typedRef);
    auto begin = IsDescSort() ? randomIter->rbegin() : randomIter->begin();
    auto end = IsDescSort() ? randomIter->rend() : randomIter->end();
    decltype(begin) beginIt, endIt;
    if (!fromStr.empty()) {
        T from;
        if (!autil::StringUtil::fromString(fromStr, from)) {
            return {Status::InternalError("parse from %s failed", fromStr.c_str())};
        }
        auto cmp = [typedRef](const autil::StringView& lhs, T target) {
            T value;
            typedRef->GetValue(lhs.data(), value, nullptr);
            return value < target;
        };
        beginIt = std::lower_bound(begin, end, from, std::move(cmp));
    } else {
        beginIt = begin;
    }
    if (!toStr.empty()) {
        T to;
        if (!autil::StringUtil::fromString(toStr, to)) {
            return {Status::InternalError("parse to %s failed", toStr.c_str())};
        }
        auto cmp = [typedRef](T target, const autil::StringView& rhs) {
            T value;
            typedRef->GetValue(rhs.data(), value, nullptr);
            return target < value;
        };
        endIt = std::upper_bound(begin, end, to, std::move(cmp));
    } else {
        endIt = end;
    }
    return {RandomAccessValueIterator::MakeSlice(std::move(randomIter), beginIt, endIt)};
}

namespace {
template <typename T>
static bool Convert(const std::string& str, T& value, const T defaultValue)
{
    if (!str.empty()) {
        return autil::StringUtil::fromString<T>(str, value);
    } else {
        value = defaultValue;
        return true;
    }
}
} // namespace

template <typename T>
StatusOr<std::unique_ptr<IValueIterator>>
RangeIteratorCreator::CreateFilterIterator(std::unique_ptr<IValueIterator> raw, const std::string& fromStr,
                                           const std::string& toStr) const
{
    auto* typedRef = dynamic_cast<const AttributeReferenceTyped<T>*>(_rangeFieldRef);
    assert(typedRef);
    T from, to;
    if (!Convert(fromStr, from, std::numeric_limits<T>::min())) {
        return Status::InternalError("convert from %s failed", fromStr.c_str());
    }
    if (!Convert(toStr, to, std::numeric_limits<T>::max())) {
        return Status::InternalError("convert to %s failed", toStr.c_str());
    }
    using Filter = Range<T>;
    auto filter = std::make_unique<Filter>(typedRef, from, to);
    return {std::make_unique<ValueIteratorWithFilter<Filter>>(std::move(raw), std::move(filter))};
}

} // namespace indexlibv2::index
