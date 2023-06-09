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
#ifndef __INDEXLIB_TRUNCATE_ATTRIBUTE_READER_H
#define __INDEXLIB_TRUNCATE_ATTRIBUTE_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class TruncateAttributeReader : public AttributeSegmentReader
{
public:
    TruncateAttributeReader();
    ~TruncateAttributeReader();
    struct ReadContext : public ReadContextBase {
        std::vector<ReadContextBasePtr> segCtx;
    };

public:
    bool Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen, bool& isNull) override;
    bool IsInMemory() const override
    {
        assert(false);
        return false;
    }
    uint32_t TEST_GetDataLength(docid_t docId) const override
    {
        assert(false);
        return 0;
    }
    uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const override
    {
        assert(false);
        return 0;
    }
    bool Updatable() const override
    {
        assert(false);
        return false;
    }

    void Init(const ReclaimMapPtr& relaimMap, const config::AttributeConfigPtr& attrConfig);
    void AddAttributeSegmentReader(segmentid_t segId, const AttributeSegmentReaderPtr& segmentReader);
    config::AttributeConfigPtr GetAttributeConfig() const { return mAttrConfig; }
    AttributeSegmentReader::ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool* pool) const override;

private:
    // TODO: it'll use too much memory while inc building many times
    typedef std::vector<AttributeSegmentReaderPtr> AttributeSegmentReaderMap;
    AttributeSegmentReaderMap mReaders;
    ReclaimMapPtr mReclaimMap;
    config::AttributeConfigPtr mAttrConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateAttributeReader);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNCATE_ATTRIBUTE_READER_H
