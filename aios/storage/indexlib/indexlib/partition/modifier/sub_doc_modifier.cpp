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
#include "indexlib/partition/modifier/sub_doc_modifier.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/modifier/main_sub_modifier_util.h"
#include "indexlib/partition/modifier/offline_modifier.h"
#include "indexlib/partition/modifier/partition_modifier_task_item.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlibv2::index;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocModifier);

SubDocModifier::SubDocModifier(const IndexPartitionSchemaPtr& schema) : PartitionModifier(schema) {}

SubDocModifier::~SubDocModifier() {}

void SubDocModifier::Init(const PartitionDataPtr& partitionData, bool enablePackFile, bool isOffline)
{
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        INDEXLIB_FATAL_ERROR(Runtime, "cannot create SubDocModifier without sub schema");
    }

    mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
    mBuildResourceMetrics->Init();

    mMainModifier = CreateSingleModifier(mSchema, partitionData, enablePackFile, isOffline);
    mSubModifier = CreateSingleModifier(subSchema, partitionData->GetSubPartitionData(), enablePackFile, isOffline);

    mMainJoinAttributeReader =
        AttributeReaderFactory::CreateJoinAttributeReader(mSchema, MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, partitionData);
    assert(mMainJoinAttributeReader);

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema && indexSchema->HasPrimaryKeyIndex());
    mMainPkIndexFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
}

void SubDocModifier::Init(const IndexPartitionReaderPtr& partitionReader)
{
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        INDEXLIB_FATAL_ERROR(Runtime, "cannot create SubDocModifier without sub schema");
    }

    mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
    mBuildResourceMetrics->Init();

    mMainModifier = CreateSingleModifier(mSchema, partitionReader);
    mSubModifier = CreateSingleModifier(subSchema, partitionReader->GetSubPartitionReader());

    mMainJoinAttributeReader = DYNAMIC_POINTER_CAST(
        JoinDocidAttributeReader, partitionReader->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME));
    assert(mMainJoinAttributeReader);

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema && indexSchema->HasPrimaryKeyIndex());
    mMainPkIndexFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
}

PartitionModifierPtr SubDocModifier::CreateSingleModifier(const IndexPartitionSchemaPtr& schema,
                                                          const PartitionDataPtr& partitionData, bool enablePackFile,
                                                          bool isOffline)
{
    assert(schema);
    assert(partitionData);
    PatchModifierPtr modifier;
    if (isOffline) {
        modifier.reset(new OfflineModifier(schema, enablePackFile));
    } else {
        modifier.reset(new PatchModifier(schema, enablePackFile));
    }
    modifier->Init(partitionData, mBuildResourceMetrics);
    return modifier;
}

PartitionModifierPtr SubDocModifier::CreateSingleModifier(const IndexPartitionSchemaPtr& schema,
                                                          const IndexPartitionReaderPtr& reader)
{
    assert(schema);
    assert(reader);
    InplaceModifierPtr modifier(new InplaceModifier(schema));
    modifier->Init(reader, reader->GetPartitionData()->GetInMemorySegment(), mBuildResourceMetrics);
    return modifier;
}

// Main-Sub doc case only supports limited  update for sub doc.
// If main doc has sub doc 1,2,3,4
// We support cases like:
// update 1,3
// update 1,2,3,4
// delete 2
// delete 1,2,3,4
// Delete is only mark delete, the deleted doc id isn't  reused.
// Adding sub doc 5 is not supported.
// i.e. Updating main doc's corresponding sub doc range after ADD is not supported.
// As a result, we don't need to update mainJoinAttribute after main-sub doc is added.
bool SubDocModifier::UpdateDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    bool ret = true;
    for (size_t i = 0; i < subDocuments.size(); ++i) {
        // if (!mSubModifier->UpdateDocument(subDocuments[i])) {
        //     IE_LOG(WARN, "failed to update sub doc, OriginalOperateType [%d]", doc->GetOriginalOperateType());
        //     ERROR_COLLECTOR_LOG(WARN, "failed to update sub doc, OriginalOperateType [%d]",
        //                         doc->GetOriginalOperateType());
        //     ret = false;
        // }
        ret = ret and mSubModifier->UpdateDocument(subDocuments[i]);
        assert(ret);
    }

    if (!mMainModifier->UpdateDocument(doc)) {
        return false;
    }

    return ret;
}

bool SubDocModifier::UpdateField(const AttrFieldValue& value)
{
    if (value.IsSubDocId()) {
        return mSubModifier->UpdateField(value);
    } else {
        return mMainModifier->UpdateField(value);
    }
    return false;
}

bool SubDocModifier::DedupDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    doc->ModifyDocOperateType(DELETE_DOC);
    RemoveMainDocument(doc);
    RemoveDupSubDocument(doc);
    doc->ModifyDocOperateType(ADD_DOC);
    return true;
}

bool SubDocModifier::RemoveDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    DocOperateType opType = doc->GetDocOperateType();
    // DELETE_DOC will delete main and all its sub docs.
    if (opType == DELETE_DOC) {
        return RemoveMainDocument(doc);
    }
    // DELETE_SUB_DOC will delete sub docs that are saved in doc->GetSubDocuments().
    if (opType == DELETE_SUB_DOC) {
        return RemoveSubDocument(doc);
    }
    assert(false);
    return false;
}

bool SubDocModifier::RemoveMainDocument(const NormalDocumentPtr& doc)
{
    if (!doc->HasPrimaryKey()) {
        // TODO: add doc will print log twice
        IE_LOG(WARN, "document has no pk");
        ERROR_COLLECTOR_LOG(WARN, "document has no pk");
        return false;
    }

    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    if (!indexDoc) {
        IE_LOG(WARN, "document has index document");
        return false;
    }

    const string& pk = indexDoc->GetPrimaryKey();
    PrimaryKeyIndexReaderPtr mainPkIndexReader = mMainModifier->GetPrimaryKeyIndexReader();
    assert(mainPkIndexReader);
    docid_t mainDocId = mainPkIndexReader->Lookup(pk);
    segmentid_t segmentId = mMainModifier->GetPartitionInfo()->GetSegmentId(mainDocId);
    doc->SetSegmentIdBeforeModified(segmentId);
    // return RemoveDocument(mainDocId);
    bool res = RemoveDocument(mainDocId);
    assert(res);
    return res;
}

bool SubDocModifier::RemoveSubDocument(const NormalDocumentPtr& doc)
{
    bool ret = true;
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); ++i) {
        if (!mSubModifier->RemoveDocument(subDocuments[i])) {
            ret = false;
        }
        assert(ret);
    }
    return ret;
}

void SubDocModifier::RemoveDupSubDocument(const NormalDocumentPtr& doc) const
{
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); ++i) {
        const NormalDocumentPtr& subDoc = subDocuments[i];
        if (mSubModifier->RemoveDocument(subDoc)) {
            IE_LOG(WARN, "main pk [%s] not dup, but sub pk [%s] dup", doc->GetPrimaryKey().c_str(),
                   subDoc->GetPrimaryKey().c_str());
        }
    }
}

bool SubDocModifier::RemoveDocument(docid_t docId)
{
    if (docId == INVALID_DOCID) {
        // TODO:
        return false;
    }

    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    MainSubModifierUtil::GetSubDocIdRange(mMainJoinAttributeReader.get(), docId, &subDocIdBegin, &subDocIdEnd);

    // [Begin, end)
    for (docid_t i = subDocIdBegin; i < subDocIdEnd; ++i) {
        if (!mSubModifier->RemoveDocument(i)) {
            IE_LOG(ERROR, "failed to delete sub document [%d]", i);
            ERROR_COLLECTOR_LOG(ERROR, "failed to delete sub document [%d]", i);
        }
    }
    return mMainModifier->RemoveDocument(docId);
}

void SubDocModifier::Dump(const DirectoryPtr& directory, segmentid_t segId)
{
    // TODO: when build enablePackageFile, get sub segment Directory from main segment Directory will lead to sub
    // segment patch file included in main segment package file. because they use different inMemorySegment, lead to
    // different storage
    DirectoryPtr subDirectory = directory->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
    assert(subDirectory);
    mSubModifier->Dump(subDirectory, segId);
    mMainModifier->Dump(directory, segId);
}

PartitionModifierDumpTaskItemPtr SubDocModifier::GetDumpTaskItem(const PartitionModifierPtr& modifier) const
{
    // Actually I think shared_from_this is better way as the once more dynamic_cast is useless
    SubDocModifierPtr subDocModifier = dynamic_pointer_cast<SubDocModifier>(modifier);
    assert(subDocModifier != nullptr);
    PartitionModifierDumpTaskItemPtr taskItem(make_shared<PartitionModifierDumpTaskItem>());
    taskItem->taskType = PartitionModifierDumpTaskType::SubDocModifierTask;
    taskItem->buildResourceMetrics = mBuildResourceMetrics;
    InplaceModifierPtr mainModifier = dynamic_pointer_cast<InplaceModifier>(subDocModifier->GetMainModifier());
    InplaceModifierPtr subModifier = dynamic_pointer_cast<InplaceModifier>(subDocModifier->GetSubModifier());
    if (mainModifier != nullptr && subModifier != nullptr) {
        // sub modifier must before main modifier
        taskItem->packAttrUpdataBitmapItems.emplace_back(subModifier->GetPackAttrUpdataBitmapVec());
        taskItem->packAttrUpdataBitmapItems.emplace_back(mainModifier->GetPackAttrUpdataBitmapVec());
        return taskItem;
    }
    // sub modifier must before main modifier
    taskItem->isOnline = false;
    taskItem->patchModifiers.emplace_back(subDocModifier->GetSubModifier());
    taskItem->patchModifiers.emplace_back(subDocModifier->GetMainModifier());
    return taskItem;
}
bool SubDocModifier::UpdateIndex(IndexUpdateTermIterator* iterator)
{
    if (iterator->IsSub()) {
        return mSubModifier->UpdateIndex(iterator);
    } else {
        return mMainModifier->UpdateIndex(iterator);
    }
}
}} // namespace indexlib::partition
