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
#ifndef __INDEXLIB_PARTITION_MODIFIER_H
#define __INDEXLIB_PARTITION_MODIFIER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, AttrFieldValue);
DECLARE_REFERENCE_CLASS(index, DefaultAttributeFieldAppender);
DECLARE_REFERENCE_CLASS(index, AttributeDocumentFieldExtractor);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, AttributeDocumentFieldExtractor);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(partition, PartitionModifierDumpTaskItem);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlib::index {
class IndexUpdateTermIterator;
}

namespace indexlib { namespace partition {

class PartitionModifier
{
public:
    PartitionModifier(const config::IndexPartitionSchemaPtr& schema);
    virtual ~PartitionModifier() {}

public:
    virtual bool DedupDocument(const document::DocumentPtr& doc) = 0;
    virtual bool UpdateDocument(const document::DocumentPtr& doc) = 0;
    virtual bool RemoveDocument(const document::DocumentPtr& doc) = 0;
    virtual void Dump(const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId) = 0;

    virtual bool IsDirty() const = 0;

    virtual bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) = 0;

    virtual bool UpdatePackField(docid_t docId, packattrid_t packAttrId, const autil::StringView& value) = 0;

    virtual bool UpdateField(const index::AttrFieldValue& value);
    virtual bool RedoIndex(docid_t docId, const document::ModifiedTokens& modifiedTokens) = 0;
    virtual bool UpdateIndex(index::IndexUpdateTermIterator* iterator) = 0;

    virtual bool RemoveDocument(docid_t docId) = 0;
    virtual docid_t GetBuildingSegmentBaseDocId() const = 0;
    virtual const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const = 0;
    virtual index::PartitionInfoPtr GetPartitionInfo() const = 0;
    virtual void SetDumpThreadNum(uint32_t dumpThreadNum) { mDumpThreadNum = dumpThreadNum; }

    bool SupportAutoAdd2Update() const { return mSupportAutoUpdate; }
    const util::BuildResourceMetricsPtr& GetBuildResourceMetrics() const;
    virtual PartitionModifierDumpTaskItemPtr
    GetDumpTaskItem(const std::shared_ptr<PartitionModifier>& modifier) const = 0;

public:
    static docid_t CalculateBuildingSegmentBaseDocId(const index_base::PartitionDataPtr& partitionData);

public: // for test
    int64_t GetTotalMemoryUse() const;

protected:
    config::IndexPartitionSchemaPtr mSchema;
    bool mSupportAutoUpdate;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    uint32_t mDumpThreadNum;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionModifier);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_MODIFIER_H
