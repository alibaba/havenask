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

#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"
#include "indexlib/index/kkv/dump/SKeyCollectInfo.h"

namespace indexlibv2::index {

class KKVDocFieldComparator
{
protected:
    using CollectInfo = SKeyCollectInfo;

public:
    KKVDocFieldComparator(bool isDesc) : _desc(isDesc) {}
    virtual ~KKVDocFieldComparator() {}

public:
    virtual bool Compare(CollectInfo*& lft, CollectInfo*& rht) = 0;

protected:
    bool _desc;
};

///////////////////////////////////////////////////////////////////////

class TimestampComparator : public KKVDocFieldComparator
{
public:
    TimestampComparator(bool isDesc) : KKVDocFieldComparator(isDesc) {}
    bool Compare(CollectInfo*& lft, CollectInfo*& rht) override
    {
        // TODO(chekong.ygm): 将mDesc模板化?
        return !_desc ? lft->ts < rht->ts : rht->ts < lft->ts;
    }
};

template <typename SKeyType>
class SKeyFieldComparator : public KKVDocFieldComparator
{
public:
    SKeyFieldComparator(bool isDesc) : KKVDocFieldComparator(isDesc) {}
    bool Compare(CollectInfo*& lft, CollectInfo*& rht) override
    {
        // TODO(chekong.ygm): 将mDesc模板化?
        return !_desc ? (SKeyType)lft->skey < (SKeyType)rht->skey : (SKeyType)rht->skey < (SKeyType)lft->skey;
    }
};

class SingleValueComparator : public KKVDocFieldComparator
{
public:
    SingleValueComparator(indexlibv2::index::AttributeReference* ref, bool desc)
        : KKVDocFieldComparator(desc)
        , _ref(ref)
    {
        assert(ref);
    }

    bool Compare(CollectInfo*& lft, CollectInfo*& rht) override
    {
        // TODO(chekong.ygm): 将mDesc模板化?
        return !_desc ? _ref->LessThan(lft->value.data(), rht->value.data())
                      : _ref->LessThan(rht->value.data(), lft->value.data());
    }

private:
    indexlibv2::index::AttributeReference* _ref;
};
} // namespace indexlibv2::index
