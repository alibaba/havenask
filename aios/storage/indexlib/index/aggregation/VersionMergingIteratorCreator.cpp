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
#include "indexlib/index/aggregation/VersionMergingIteratorCreator.h"

#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/aggregation/SortedVersionMergingIterator.h"
#include "indexlib/index/aggregation/ValueIteratorWithFilter.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"

namespace indexlibv2::index {

StatusOr<std::unique_ptr<IValueIterator>>
VersionMergingIteratorCreator::Create(DataIterator dataIter, DataIterator delDataIter, autil::mem_pool::PoolBase* pool)
{
#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        using T = typename indexlib::IndexlibFieldType2CppType<ft, false>::CppType;                                    \
        return CreateWithTypedUniqueField<T>(std::move(dataIter), std::move(delDataIter), pool);                       \
    }
    const AttributeReference* ref = dataIter.uniqueFieldRef;
    switch (ref->GetFieldType()) {
        NUMERIC_FIELD_TYPE_MACRO_HELPER(CASE);
    default:
        return {Status::Unimplement("unsupported field type: %d", (int)ref->GetFieldType())};
    }
#undef CASE
}

template <typename T>
StatusOr<std::unique_ptr<IValueIterator>>
VersionMergingIteratorCreator::CreateWithTypedUniqueField(DataIterator dataIter, DataIterator delDataIter,
                                                          autil::mem_pool::PoolBase* pool)
{
#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        using T2 = typename indexlib::IndexlibFieldType2CppType<ft, false>::CppType;                                   \
        return CreateImpl<T, T2>(std::move(dataIter), std::move(delDataIter), pool);                                   \
    }
    const AttributeReference* ref = dataIter.versionFieldRef;
    switch (ref->GetFieldType()) {
        NUMERIC_FIELD_TYPE_MACRO_HELPER(CASE);
    default:
        return {Status::Unimplement("unsupported field type: %d", (int)ref->GetFieldType())};
    }
#undef CASE
}

template <typename T1, typename T2>
static Status CollectDeletedVersion(IValueIterator* iter, const AttributeReference* uniqueFieldRef,
                                    const AttributeReference* versionFieldRef, autil::mem_pool::PoolBase* pool,
                                    std::unordered_map<T1, T2>& deletedVersion)
{
    auto uniqueFieldRefTyped = dynamic_cast<const AttributeReferenceTyped<T1>*>(uniqueFieldRef);
    auto versionFieldRefTyped = dynamic_cast<const AttributeReferenceTyped<T2>*>(versionFieldRef);
    if (!uniqueFieldRefTyped || !versionFieldRefTyped) {
        return Status::InternalError("type inconsistent");
    }
    while (iter->HasNext()) {
        autil::StringView value;
        auto s = iter->Next(pool, value);
        if (!s.IsOK() && !s.IsEof()) {
            return s;
        } else if (s.IsEof()) {
            break;
        }
        T1 key;
        T2 version;
        uniqueFieldRefTyped->GetValue(value.data(), key, nullptr);
        versionFieldRefTyped->GetValue(value.data(), version, nullptr);
        auto it = deletedVersion.find(key);
        if (it == deletedVersion.end() || it->second < version) {
            deletedVersion[key] = version;
        }
    }
    return Status::OK();
}

template <typename T1, typename T2>
StatusOr<std::unique_ptr<IValueIterator>> VersionMergingIteratorCreator::CreateImpl(DataIterator dataIter,
                                                                                    DataIterator delDataIter,
                                                                                    autil::mem_pool::PoolBase* pool)
{
    if (dataIter.sortPattern == config::sp_nosort) {
        return CreateSimple<T1, T2>(std::move(dataIter), std::move(delDataIter), pool);
    } else {
        auto uniqueFieldRefTypedA = dynamic_cast<const AttributeReferenceTyped<T1>*>(dataIter.uniqueFieldRef);
        auto versionFieldRefTypedA = dynamic_cast<const AttributeReferenceTyped<T2>*>(dataIter.versionFieldRef);
        auto uniqueFieldRefTypedB = dynamic_cast<const AttributeReferenceTyped<T1>*>(delDataIter.uniqueFieldRef);
        auto versionFieldRefTypedB = dynamic_cast<const AttributeReferenceTyped<T2>*>(delDataIter.versionFieldRef);
        if (!uniqueFieldRefTypedA || !versionFieldRefTypedA || !uniqueFieldRefTypedB || !versionFieldRefTypedB) {
            return Status::InternalError("type inconsistent");
        }
        return {std::make_unique<SortedVersionMergingIterator<T1, T2>>(
            std::move(dataIter.data), uniqueFieldRefTypedA, versionFieldRefTypedA, std::move(delDataIter.data),
            uniqueFieldRefTypedB, versionFieldRefTypedB, dataIter.sortPattern == config::sp_desc)};
    }
}

template <typename T1, typename T2>
class VersionFilter
{
public:
    VersionFilter(const AttributeReferenceTyped<T1>* keyRef, const AttributeReferenceTyped<T2>* versionRef,
                  std::unordered_map<T1, T2> deletedVersions)
        : _keyRef(keyRef)
        , _versionRef(versionRef)
        , _deletedVersions(std::move(deletedVersions))
    {
    }

public:
    bool Filter(const autil::StringView& value) const
    {
        T1 key;
        T2 version;
        _keyRef->GetValue(value.data(), key, nullptr);
        _versionRef->GetValue(value.data(), version, nullptr);
        auto it = _deletedVersions.find(key);
        return it == _deletedVersions.end() || version > it->second;
    }

private:
    const AttributeReferenceTyped<T1>* _keyRef;
    const AttributeReferenceTyped<T2>* _versionRef;
    std::unordered_map<T1, T2> _deletedVersions;
};

template <typename T1, typename T2>
StatusOr<std::unique_ptr<IValueIterator>> VersionMergingIteratorCreator::CreateSimple(DataIterator dataIter,
                                                                                      DataIterator delDataIter,
                                                                                      autil::mem_pool::PoolBase* pool)
{
    std::unordered_map<T1, T2> deletedVersion;
    auto s = CollectDeletedVersion(delDataIter.data.get(), delDataIter.uniqueFieldRef, delDataIter.versionFieldRef,
                                   pool, deletedVersion);
    if (!s.IsOK()) {
        return {s.steal_error()};
    }
    if (deletedVersion.empty()) {
        return {std::move(dataIter.data)};
    }
    auto uniqueFieldRefTyped = dynamic_cast<const AttributeReferenceTyped<T1>*>(dataIter.uniqueFieldRef);
    auto versionFieldRefTyped = dynamic_cast<const AttributeReferenceTyped<T2>*>(dataIter.versionFieldRef);
    if (!uniqueFieldRefTyped || !versionFieldRefTyped) {
        return Status::InternalError("type inconsistent");
    }
    using FilterType = VersionFilter<T1, T2>;
    auto filter = std::make_unique<FilterType>(uniqueFieldRefTyped, versionFieldRefTyped, std::move(deletedVersion));
    using IteratorType = ValueIteratorWithFilter<FilterType>;
    return {std::make_unique<IteratorType>(std::move(dataIter.data), std::move(filter))};
}

} // namespace indexlibv2::index
