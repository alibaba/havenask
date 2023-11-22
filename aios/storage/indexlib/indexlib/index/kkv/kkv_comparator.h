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

#include <memory>

#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/data_collector_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KKVComparator
{
protected:
    typedef SKeyCollectInfo CollectInfo;

public:
    KKVComparator(bool isDesc) : mDesc(isDesc) {}
    virtual ~KKVComparator() {}

public:
    virtual bool Compare(CollectInfo*& lft, CollectInfo*& rht) = 0;

protected:
    bool mDesc;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVComparator);

///////////////////////////////////////////////////////////////////////

class TimestampComparator : public KKVComparator
{
public:
    TimestampComparator(bool isDesc) : KKVComparator(isDesc) {}
    bool Compare(CollectInfo*& lft, CollectInfo*& rht) override
    {
        return !mDesc ? lft->ts < rht->ts : rht->ts < lft->ts;
    }
};

template <typename SKeyType>
class SKeyComparator : public KKVComparator
{
public:
    SKeyComparator(bool isDesc) : KKVComparator(isDesc) {}
    bool Compare(CollectInfo*& lft, CollectInfo*& rht) override
    {
        return !mDesc ? (SKeyType)lft->skey < (SKeyType)rht->skey : (SKeyType)rht->skey < (SKeyType)lft->skey;
    }
};

class SingleValueComparator : public KKVComparator
{
public:
    SingleValueComparator(common::AttributeReference* ref, bool desc) : KKVComparator(desc), mRef(ref) { assert(ref); }

    bool Compare(CollectInfo*& lft, CollectInfo*& rht) override
    {
        return !mDesc ? mRef->LessThan(lft->value.data(), rht->value.data())
                      : mRef->LessThan(rht->value.data(), lft->value.data());
    }

private:
    common::AttributeReference* mRef;
};
}} // namespace indexlib::index
