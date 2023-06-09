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
#ifndef __INDEXLIB_PACK_ATTRIBUTE_PATCH_READER_H
#define __INDEXLIB_PACK_ATTRIBUTE_PATCH_READER_H

#include <memory>
#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

typedef std::pair<AttributePatchReader*, attrid_t> PatchReaderItem;

class PatchReaderItemComparator
{
public:
    bool operator()(const PatchReaderItem& lhs, const PatchReaderItem& rhs)
    {
        docid_t lDocId = lhs.first->GetCurrentDocId();
        docid_t rDocId = rhs.first->GetCurrentDocId();
        if (lDocId != rDocId) {
            return lDocId > rDocId;
        }
        return lhs.second > rhs.second; // compare attrid
    }
};

class PackAttributePatchReader : public AttributePatchReader
{
public:
    PackAttributePatchReader(const config::PackAttributeConfigPtr& packAttrConfig)
        : AttributePatchReader(packAttrConfig->CreateAttributeConfig())
        , mPackAttrConfig(packAttrConfig)
        , mPatchItemCount(0)
    {
    }

    ~PackAttributePatchReader();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, segmentid_t segmentId) override;

    void AddPatchFile(const file_system::DirectoryPtr& directory, const std::string& fileName,
                      segmentid_t srcSegmentId) override
    {
        assert(false);
    }

    docid_t GetCurrentDocId() const override
    {
        assert(HasNext());
        return mHeap.top().first->GetCurrentDocId();
    }

    size_t Next(docid_t& docId, uint8_t* value, size_t maxLen, bool& isNull) override;

    size_t Seek(docid_t docId, uint8_t* value, size_t maxLen) override;

    bool HasNext() const override { return !mHeap.empty(); }

    uint32_t GetMaxPatchItemLen() const override;

    size_t GetPatchFileLength() const override
    {
        assert(false);
        return 0;
    };

    size_t GetPatchItemCount() const override { return mPatchItemCount; }

private:
    AttributePatchReader* CreateSubAttributePatchReader(const config::AttributeConfigPtr& attrConfig,
                                                        const index_base::PartitionDataPtr& partitionData,
                                                        segmentid_t segmentId);

    void PushBackToHeap(const PatchReaderItem& readerItem);

private:
    typedef std::priority_queue<PatchReaderItem, std::vector<PatchReaderItem>, PatchReaderItemComparator>
        PatchReaderItemHeap;

private:
    PatchReaderItemHeap mHeap;
    std::vector<AttrFieldValuePtr> mPatchValues;
    config::PackAttributeConfigPtr mPackAttrConfig;
    size_t mPatchItemCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributePatchReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_PACK_ATTRIBUTE_PATCH_READER_H
