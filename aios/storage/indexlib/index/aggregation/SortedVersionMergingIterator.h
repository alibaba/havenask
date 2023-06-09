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

#include <optional>

#include "autil/StringView.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"

namespace indexlibv2::index {
namespace {
class IteratorQueueAdapter
{
public:
    IteratorQueueAdapter(std::unique_ptr<IValueIterator> iter) : _iter(std::move(iter)) {}

public:
    bool Empty() const { return !_cur && !_iter->HasNext(); }

    Status Top(autil::mem_pool::PoolBase* pool, autil::StringView& data)
    {
        if (!_cur) {
            autil::StringView value;
            auto s = _iter->Next(pool, value);
            if (!s.IsOK()) {
                return s;
            }
            _cur = value;
        }
        data = _cur.value();
        return Status::OK();
    }

    void Pop() { _cur.reset(); }

private:
    std::optional<autil::StringView> _cur;
    std::unique_ptr<IValueIterator> _iter;
};

template <typename T>
struct Comparator {
    const AttributeReferenceTyped<T>* refL = nullptr;
    const AttributeReferenceTyped<T>* refR = nullptr;
    bool desc = true;

    int Compare(const autil::StringView& lhs, const autil::StringView& rhs) const
    {
        T left, right;
        refL->GetValue(lhs.data(), left, nullptr);
        refR->GetValue(rhs.data(), right, nullptr);
        if (left == right) {
            return 0;
        } else {
            bool lt = !desc ? left < right : right < left;
            return lt ? -1 : 1;
        }
    }
};

} // namespace

template <typename T1, typename T2>
class MergedVersionIterator final : public IValueIterator
{
public:
    MergedVersionIterator(std::unique_ptr<IValueIterator> impl, const AttributeReferenceTyped<T1>* keyRef,
                          const AttributeReferenceTyped<T2>* versionRef)
        : _impl(std::move(impl))
        , _keyRef(keyRef)
        , _versionRef(versionRef)
    {
    }

public:
    bool HasNext() const override { return _lastKey || _impl->HasNext(); }

    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        while (_impl->HasNext()) {
            auto s = _impl->Next(pool, value);
            if (!s.IsOK() && !s.IsEof()) {
                return s;
            } else if (s.IsEof()) {
                break;
            } else {
                T1 key;
                T2 version;
                _keyRef->GetValue(value.data(), key, nullptr);
                _versionRef->GetValue(value.data(), version, nullptr);
                if (!_lastKey) {
                    _lastKey = key;
                    _maxVersion = version;
                    _maxVersionRecord = value;
                } else {
                    if (_lastKey.value() != key) {
                        _lastKey = key;
                        _maxVersion = version;
                        std::swap(value, _maxVersionRecord);
                        return Status::OK();
                    } else if (version > _maxVersion) {
                        _maxVersion = version;
                        _maxVersionRecord = value;
                    }
                }
            }
        }
        if (!_lastKey) {
            return Status::Eof();
        }
        value = std::move(_maxVersionRecord);
        _lastKey.reset();
        return Status::OK();
    }

private:
    std::unique_ptr<IValueIterator> _impl;
    const AttributeReferenceTyped<T1>* _keyRef;
    const AttributeReferenceTyped<T2>* _versionRef;
    std::optional<T1> _lastKey;
    T2 _maxVersion;
    autil::StringView _maxVersionRecord;
};

template <typename T1, typename T2>
class SortedVersionMergingIterator final : public IValueIterator
{
public:
    // assumption: A and B are sorted
    SortedVersionMergingIterator(std::unique_ptr<IValueIterator> a, const AttributeReferenceTyped<T1>* uniqueRefA,
                                 const AttributeReferenceTyped<T2>* versionRefA, std::unique_ptr<IValueIterator> b,
                                 const AttributeReferenceTyped<T1>* uniqueRefB,
                                 const AttributeReferenceTyped<T2>* versionRefB, bool desc)
        : _a(std::make_unique<MergedVersionIterator<T1, T2>>(std::move(a), uniqueRefA, versionRefA))
        , _b(std::make_unique<MergedVersionIterator<T1, T2>>(std::move(b), uniqueRefB, versionRefB))
        , _cmp {uniqueRefA, uniqueRefB, desc}
        , _versionRefA(versionRefA)
        , _versionRefB(versionRefB)
    {
    }

public:
    bool HasNext() const override { return !_a.Empty(); }

    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        while (!_a.Empty() && !_b.Empty()) {
            autil::StringView valueA, valueB;
            auto s1 = _a.Top(pool, valueA);
            if (!s1.IsOK()) {
                return s1;
            }
            _a.Pop();
            auto s2 = _b.Top(pool, valueB);
            if (s2.IsEof()) {
                value = std::move(valueA);
                return Status::OK();
            } else if (!s2.IsOK()) {
                return s2;
            }
            int r = _cmp.Compare(valueA, valueB);
            if (r >= 0) {
                _b.Pop();
            }
            if (r == 0) {
                T2 versionA, versionB;
                _versionRefA->GetValue(valueA.data(), versionA, nullptr);
                _versionRefB->GetValue(valueB.data(), versionB, nullptr);
                if (versionA > versionB) {
                    value = std::move(valueA);
                    return Status::OK();
                }
            } else {
                value = std::move(valueA);
                return Status::OK();
            }
        }
        if (!_a.Empty()) {
            auto s = _a.Top(pool, value);
            _a.Pop();
            return s;
        }
        return Status::Eof();
    }

private:
    IteratorQueueAdapter _a;
    IteratorQueueAdapter _b;
    Comparator<T1> _cmp;
    const AttributeReferenceTyped<T2>* _versionRefA;
    const AttributeReferenceTyped<T2>* _versionRefB;
};

} // namespace indexlibv2::index
