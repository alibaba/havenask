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
#include "indexlib/index/aggregation/Aggregator.h"

#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/aggregation/AggregationFunctor.h"
#include "indexlib/index/aggregation/AggregationKeyHasher.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlibv2::index {

Aggregator::Aggregator(const AggregationReader* reader, autil::mem_pool::Pool* pool)
    : _reader(reader)
    , _pool(pool)
    , _iterCreator(std::make_unique<ValueIteratorCreator>(reader))
{
}

Aggregator::~Aggregator() = default;

Aggregator& Aggregator::Key(uint64_t key)
{
    _key = key;
    return *this;
}

Aggregator& Aggregator::Keys(const std::vector<autil::StringView>& rawKeys)
{
    _key = AggregationKeyHasher::Hash(rawKeys);
    return *this;
}

Aggregator& Aggregator::Keys(const std::vector<std::string>& rawKeys)
{
    _key = AggregationKeyHasher::Hash(rawKeys);
    return *this;
}

Aggregator& Aggregator::FilterBy(const std::string& field, const std::string& from, const std::string& to)
{
    _iterCreator->FilterBy(field, from, to);
    return *this;
}

autil::Result<uint64_t> Aggregator::Count() const
{
    const auto& indexConfig = _reader->GetIndexConfig();
    if (!indexConfig->SupportDelete()) {
        auto meta = _reader->GetMergedValueMeta(_key);
        return {meta.valueCount};
    }
    _iterCreator->Hint(ValueIteratorCreator::IH_SEQ);
    auto r = _iterCreator->CreateIterator(_key, _pool);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    CountFunctor counter;
    return Aggregate<CountFunctor>(std::move(r.steal_value()), std::move(counter));
}

autil::Result<uint64_t> Aggregator::DistinctCount(const std::string& fieldName) const
{
    auto r = _iterCreator->CreateDistinctIterator(_key, _pool, fieldName);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    CountFunctor counter;
    return Aggregate<CountFunctor>(std::move(r.steal_value()), std::move(counter));
}

autil::Result<double> Aggregator::Sum(const std::string& fieldName) const
{
    _iterCreator->Hint(ValueIteratorCreator::IH_SEQ);
    auto r = _iterCreator->CreateIterator(_key, _pool);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    return SumImpl(std::move(r.steal_value()), fieldName);
}

autil::Result<double> Aggregator::DistinctSum(const std::string& sumField, const std::string& distinctField) const
{
    auto r = _iterCreator->CreateDistinctIterator(_key, _pool, distinctField);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    return SumImpl(std::move(r.steal_value()), sumField);
}

autil::Result<double> Aggregator::SumImpl(std::unique_ptr<IValueIterator> iter, const std::string& fieldName) const
{
    const auto* packAttrFormatter = _reader->GetPackAttributeFormatter();
    assert(packAttrFormatter);
    AttributeReference* attrRef = packAttrFormatter->GetAttributeReference(fieldName);
    if (attrRef == nullptr) {
        return autil::result::RuntimeError::make("field %s does not exist in value", fieldName.c_str());
    }
    if (attrRef->IsMultiValue() || !indexlib::IsNumericField(attrRef->GetFieldType())) {
        return autil::result::RuntimeError::make("field %s is not a single numeric type", fieldName.c_str());
    }

#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        using T = typename indexlib::IndexlibFieldType2CppType<ft, false>::CppType;                                    \
        AttributeReferenceTyped<T>* typedRef = dynamic_cast<AttributeReferenceTyped<T>*>(attrRef);                     \
        using F = SumFunctor<T, double>;                                                                               \
        F f(typedRef);                                                                                                 \
        auto r = Aggregate<F>(std::move(iter), std::move(f));                                                          \
        if (!r.is_ok()) {                                                                                              \
            return {r.steal_error()};                                                                                  \
        }                                                                                                              \
        return {r.steal_value()};                                                                                      \
    }

    switch (attrRef->GetFieldType()) {
        NUMERIC_FIELD_TYPE_MACRO_HELPER(CASE);
    default:
        return {Status::Unimplement("unsupported field type: %d", (int)attrRef->GetFieldType())};
    }

#undef CASE
}

template <typename Functor>
autil::Result<typename Functor::ResultType> Aggregator::Aggregate(std::unique_ptr<IValueIterator> iter, Functor f) const
{
    while (iter->HasNext()) {
        autil::StringView value;
        auto s = iter->Next(_pool, value);
        if (s.IsOK()) {
            f(value);
        } else if (s.IsEof()) {
            break;
        } else {
            return {s.steal_error()};
        }
    }
    return {f.GetResult()};
}

} // namespace indexlibv2::index
