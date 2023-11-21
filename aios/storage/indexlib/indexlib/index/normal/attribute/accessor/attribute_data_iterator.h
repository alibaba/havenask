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

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlib { namespace index {

class AttributeDataIterator
{
public:
    AttributeDataIterator(const config::AttributeConfigPtr& attrConfig);
    virtual ~AttributeDataIterator();

public:
    virtual bool Init(const index_base::PartitionDataPtr& partData, segmentid_t segId) = 0;
    virtual void MoveToNext() = 0;
    virtual std::string GetValueStr() const = 0;

    // from T & MultiValue<T>
    virtual autil::StringView GetValueBinaryStr(autil::mem_pool::Pool* pool) const = 0;

    // from raw index data, maybe not MultiValue<T> (compress float)
    // attention: return value lifecycle only valid in current value pos,
    //            which will be changed when call MoveToNext
    virtual autil::StringView GetRawIndexImageValue() const = 0;

    virtual bool IsNullValue() const = 0;

    bool IsValid() const { return mCurDocId < mDocCount; }

    config::AttributeConfigPtr GetAttrConfig() const { return mAttrConfig; }
    uint32_t GetDocCount() const { return (uint32_t)mDocCount; }

protected:
    config::AttributeConfigPtr mAttrConfig;
    docid_t mDocCount;
    docid_t mCurDocId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataIterator);
}} // namespace indexlib::index
