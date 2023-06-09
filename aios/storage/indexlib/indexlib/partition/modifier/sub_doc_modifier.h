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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index, JoinDocidAttributeReader);

namespace indexlib { namespace partition {

class SubDocModifier : public PartitionModifier
{
public:
    SubDocModifier(const config::IndexPartitionSchemaPtr& schema);
    ~SubDocModifier();

public:
    // for offline & patch modifier
    void Init(const index_base::PartitionDataPtr& partitionData, bool enablePackFile, bool isOffline);

    // for online
    void Init(const IndexPartitionReaderPtr& partitionReader);
    bool UpdateDocument(const document::DocumentPtr& doc) override;
    bool DedupDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(const document::DocumentPtr& doc) override;

    void Dump(const file_system::DirectoryPtr& directory, segmentid_t segId) override;

    bool IsDirty() const override { return mMainModifier->IsDirty() || mSubModifier->IsDirty(); }

    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override
    {
        assert(false);
        return false;
    }

    bool UpdatePackField(docid_t docId, packattrid_t packAttrId, const autil::StringView& value) override
    {
        assert(false);
        return false;
    }

    bool UpdateField(const index::AttrFieldValue& value) override;

    bool RedoIndex(docid_t docId, const document::ModifiedTokens& modifiedTokens) override
    {
        assert(false);
        return false;
    }
    bool UpdateIndex(index::IndexUpdateTermIterator* iterator) override;

    bool RemoveDocument(docid_t docId) override;

    docid_t GetBuildingSegmentBaseDocId() const override
    {
        assert(false);
        return INVALID_DOCID;
    }

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override
    {
        return mMainModifier->GetPrimaryKeyIndexReader();
    }

    index::PartitionInfoPtr GetPartitionInfo() const override { return mMainModifier->GetPartitionInfo(); }

    void SetDumpThreadNum(uint32_t dumpThreadNum) override
    {
        if (mMainModifier) {
            mMainModifier->SetDumpThreadNum(dumpThreadNum);
        }

        if (mSubModifier) {
            mSubModifier->SetDumpThreadNum(dumpThreadNum);
        }
    }

    PartitionModifierDumpTaskItemPtr GetDumpTaskItem(const PartitionModifierPtr& modifier) const override;

public:
    const PartitionModifierPtr& GetMainModifier() const { return mMainModifier; }

    const PartitionModifierPtr& GetSubModifier() const { return mSubModifier; }

    const index::JoinDocidAttributeReaderPtr& GetMainJoinAttributeReader() const { return mMainJoinAttributeReader; }

private:
    PartitionModifierPtr CreateSingleModifier(const config::IndexPartitionSchemaPtr& schema,
                                              const index_base::PartitionDataPtr& partitionData, bool enablePackFile,
                                              bool isOffline);

    PartitionModifierPtr CreateSingleModifier(const config::IndexPartitionSchemaPtr& schema,
                                              const IndexPartitionReaderPtr& reader);

    bool RemoveMainDocument(const document::NormalDocumentPtr& doc);
    bool RemoveSubDocument(const document::NormalDocumentPtr& doc);

    void RemoveDupSubDocument(const document::NormalDocumentPtr& doc) const;

protected:
    PartitionModifierPtr mMainModifier;
    PartitionModifierPtr mSubModifier;
    index::JoinDocidAttributeReaderPtr mMainJoinAttributeReader;
    fieldid_t mMainPkIndexFieldId;

private:
    friend class SubDocModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocModifier);
}} // namespace indexlib::partition
