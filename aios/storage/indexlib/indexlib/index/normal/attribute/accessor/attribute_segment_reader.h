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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index, AttributePatchReader);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);

namespace indexlib { namespace index {

class AttributeSegmentReader
{
public:
    AttributeSegmentReader(AttributeMetrics* attributeMetrics = NULL,
                           TemperatureProperty property = TemperatureProperty::UNKNOWN)
        : mAttributeMetrics(attributeMetrics)
        , mProperty(property)
        , mEnableAccessCountors(false)
        , mGlobalCtxSwitchLimit(0)
    {
    }
    virtual ~AttributeSegmentReader() {}
    struct ReadContextBase {
        ReadContextBase() = default;
        virtual ~ReadContextBase() {};
    };
    DEFINE_SHARED_PTR(ReadContextBase);

public:
    virtual bool IsInMemory() const = 0;
    virtual uint32_t TEST_GetDataLength(docid_t docId) const = 0;
    virtual uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const = 0;
    virtual bool Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen, bool& isNull) = 0;
    virtual ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool* pool) const = 0;
    virtual bool ReadDataAndLen(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen,
                                uint32_t& dataLen)
    {
        dataLen = bufLen;
        bool isNull = false;
        return Read(docId, ctx, buf, bufLen, isNull);
    }
    virtual bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull) { return false; }
    virtual bool Updatable() const = 0;
    void EnableAccessCountors() { mEnableAccessCountors = true; }
    void EnableGlobalReadContext();

    ReadContextBase* GetGlobalReadContext()
    {
        if (mGlobalCtxPool && mGlobalCtxPool->getUsedBytes() > mGlobalCtxSwitchLimit) {
            mGlobalCtx.reset();
            mGlobalCtxPool->release();
            mGlobalCtx = CreateReadContextPtr(mGlobalCtxPool.get());
        }
        return mGlobalCtx.get();
    }

protected:
    bool SupportFileCompress(const config::AttributeConfigPtr& attrConfig,
                             const index_base::SegmentInfo& segInfo) const;

protected:
    AttributeMetrics* mAttributeMetrics;
    TemperatureProperty mProperty;
    bool mEnableAccessCountors;
    autil::mem_pool::PoolPtr mGlobalCtxPool;
    ReadContextBasePtr mGlobalCtx;
    size_t mGlobalCtxSwitchLimit;
};
DEFINE_SHARED_PTR(AttributeSegmentReader);

struct AttributeSegmentReaderWithCtx {
    AttributeSegmentReaderPtr reader;
    AttributeSegmentReader::ReadContextBasePtr ctx;
};

}} // namespace indexlib::index
