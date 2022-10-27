#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/modifier/offline_modifier.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocModifier);

SubDocModifier::SubDocModifier(const IndexPartitionSchemaPtr& schema)
    : PartitionModifier(schema)
{
}

SubDocModifier::~SubDocModifier() 
{
}

void SubDocModifier::Init(const PartitionDataPtr& partitionData,
                          bool enablePackFile, bool isOffline)
{
    const IndexPartitionSchemaPtr& subSchema = 
        mSchema->GetSubIndexPartitionSchema();
    if (!subSchema)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "cannot create SubDocModifier without sub schema");
    }

    mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
    mBuildResourceMetrics->Init();
    
    mMainModifier = CreateSingleModifier(
            mSchema, partitionData, enablePackFile, isOffline);
    mSubModifier = CreateSingleModifier(
            subSchema, partitionData->GetSubPartitionData(),
            enablePackFile, isOffline);

    mMainJoinAttributeReader = AttributeReaderFactory::CreateJoinAttributeReader(
            mSchema, MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, partitionData);
    assert(mMainJoinAttributeReader);

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema && indexSchema->HasPrimaryKeyIndex());
    mMainPkIndexFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
}

void SubDocModifier::Init(const IndexPartitionReaderPtr& partitionReader)
{
    const IndexPartitionSchemaPtr& subSchema = 
        mSchema->GetSubIndexPartitionSchema();
    if (!subSchema)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "cannot create SubDocModifier without sub schema");
    }

    mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
    mBuildResourceMetrics->Init();
        
    mMainModifier = CreateSingleModifier(mSchema, partitionReader);
    mSubModifier = CreateSingleModifier(
            subSchema, partitionReader->GetSubPartitionReader());
    
    mMainJoinAttributeReader = DYNAMIC_POINTER_CAST(JoinDocidAttributeReader,
            partitionReader->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME));
    assert(mMainJoinAttributeReader);

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema && indexSchema->HasPrimaryKeyIndex());
    mMainPkIndexFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
}

PartitionModifierPtr SubDocModifier::CreateSingleModifier(
        const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData,
        bool enablePackFile, bool isOffline)
{
    assert(schema);
    assert(partitionData);
    PatchModifierPtr modifier;    
    if (isOffline)
    {
        modifier.reset(new OfflineModifier(schema, enablePackFile));
    }
    else
    {
        modifier.reset(new PatchModifier(schema, enablePackFile));
    }
    modifier->Init(partitionData, mBuildResourceMetrics);
    return modifier;
}

PartitionModifierPtr SubDocModifier::CreateSingleModifier(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionReaderPtr& reader)
{
    assert(schema);
    assert(reader);
    InplaceModifierPtr modifier(new InplaceModifier(schema));    
    modifier->Init(reader, reader->GetPartitionData()->GetInMemorySegment(),
                   mBuildResourceMetrics);
    return modifier;
}

bool SubDocModifier::UpdateDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!ValidateSubDocs(doc)) {
        return false;
    }

    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    bool needUpdateMain = NeedUpdate(doc, mMainPkIndexFieldId);
    if (subDocuments.size() == 0 && !needUpdateMain)
    {
        IE_LOG(WARN, "no field to update, OriginalOperateType [%d]",
               doc->GetOriginalOperateType());
        ERROR_COLLECTOR_LOG(WARN, "no field to update, OriginalOperateType [%d]",
               doc->GetOriginalOperateType());
        return false;
    }

    bool ret = true;
    for (size_t i = 0; i < subDocuments.size(); ++i)
    {
        if (!mSubModifier->UpdateDocument(subDocuments[i]))
        {
            IE_LOG(WARN, "failed to update sub doc, OriginalOperateType [%d]",
                   doc->GetOriginalOperateType());
            ERROR_COLLECTOR_LOG(WARN, "failed to update sub doc, OriginalOperateType [%d]",
                   doc->GetOriginalOperateType());
            ret = false;
        }
    }

    if (needUpdateMain && !mMainModifier->UpdateDocument(doc))
    {
        ret = false;
    }

    return ret;
}

bool SubDocModifier::UpdateField(const AttrFieldValue& value)
{
    if (value.IsSubDocId())
    {
        return mSubModifier->UpdateField(value);
    }
    else
    {
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
    if (opType == DELETE_DOC)
    {
        return RemoveMainDocument(doc);
    }
    else if (opType == DELETE_SUB_DOC)
    {
        return RemoveSubDocument(doc);
    }
    assert(false);
    return false;
}

bool SubDocModifier::RemoveMainDocument(const NormalDocumentPtr& doc)
{
    if (!doc->HasPrimaryKey())
    {
        //TODO: add doc will print log twice
        IE_LOG(WARN, "document has no pk");
        ERROR_COLLECTOR_LOG(WARN, "document has no pk");
        return false;
    }

    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    if (!indexDoc)
    {
        IE_LOG(WARN, "document has index document");
        return false;
    }

    const string& pk = indexDoc->GetPrimaryKey();
    PrimaryKeyIndexReaderPtr mainPkIndexReader = 
        mMainModifier->GetPrimaryKeyIndexReader();
    assert(mainPkIndexReader);
    docid_t mainDocId = mainPkIndexReader->Lookup(pk);
    segmentid_t segmentId = mMainModifier->GetPartitionInfo()->GetSegmentId(mainDocId);
    doc->SetSegmentIdBeforeModified(segmentId);
    return RemoveDocument(mainDocId);
}

void SubDocModifier::RemoveDupSubDocument(const NormalDocumentPtr& doc) const
{
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); ++i)
    {
        const NormalDocumentPtr& subDoc = subDocuments[i];
        if (mSubModifier->RemoveDocument(subDoc))
        {
            IE_LOG(WARN, "main pk [%s] not dup, but sub pk [%s] dup",
                   doc->GetPrimaryKey().c_str(),
                   subDoc->GetPrimaryKey().c_str());
        }
    }
}

docid_t SubDocModifier::GetDocId(const NormalDocumentPtr& doc,
                                 const PartitionModifierPtr& modifier) const
{
    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    const string& pk = indexDoc->GetPrimaryKey();
    PrimaryKeyIndexReaderPtr reader = modifier->GetPrimaryKeyIndexReader();
    assert(reader);
    return reader->Lookup(pk);
}

void SubDocModifier::GetSubDocIdRange(docid_t mainDocId, docid_t& subDocIdBegin,
                                      docid_t& subDocIdEnd) const
{
    subDocIdBegin = 0;
    if (mainDocId > 0)
    {
        subDocIdBegin = mMainJoinAttributeReader->GetJoinDocId(mainDocId - 1);
    }
    subDocIdEnd = mMainJoinAttributeReader->GetJoinDocId(mainDocId);
    
    if (subDocIdEnd < 0
        || subDocIdBegin < 0
        || (subDocIdBegin > subDocIdEnd))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "failed to get join docid [%d, %d), mainDocId [%d]",
                             subDocIdBegin, subDocIdEnd, mainDocId);
    }
}

bool SubDocModifier::ValidateSubDocs(const NormalDocumentPtr& doc) const
{
    docid_t mainDocId = GetDocId(doc, mMainModifier);
    if (mainDocId == INVALID_DOCID) {
        IE_LOG(TRACE1, "mainPk[%s] does not exist.", doc->GetPrimaryKey().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "mainPk[%s] does not exist.", doc->GetPrimaryKey().c_str());
        return false;
    }
    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    GetSubDocIdRange(mainDocId, subDocIdBegin, subDocIdEnd);

    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for(int32_t i = (int32_t)subDocuments.size() - 1; i >= 0; --i)
    {
        docid_t subDocId = GetDocId(subDocuments[i], mSubModifier);
        if (subDocId == INVALID_DOCID) 
        {
            continue;
        }
        if (subDocId < subDocIdBegin || subDocId >= subDocIdEnd)
        {
            IE_LOG(WARN, "sub doc not in same doc, mainPK[%s], subPK[%s]",
                   doc->GetPrimaryKey().c_str(), 
                   subDocuments[i]->GetPrimaryKey().c_str());
            ERROR_COLLECTOR_LOG(WARN, "sub doc not in same doc, mainPK[%s], subPK[%s]",
                    doc->GetPrimaryKey().c_str(), 
                    subDocuments[i]->GetPrimaryKey().c_str());
            doc->RemoveSubDocument(i);
        }
    }
    return true;
}

bool SubDocModifier::RemoveSubDocument(const NormalDocumentPtr& doc)
{
    if (!ValidateSubDocs(doc)) {
        return false;
    }

    bool ret = true;
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    if (subDocuments.size() == 0)
    {
        return false;
    }
    for (size_t i = 0; i < subDocuments.size(); ++i)
    {
        if (!mSubModifier->RemoveDocument(subDocuments[i]))
        {
            ret = false;
        }
    }
    return ret;
}

bool SubDocModifier::RemoveDocument(docid_t docId)
{
    if (docId == INVALID_DOCID)
    {
        //TODO:
        return false;
    }

    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    GetSubDocIdRange(docId, subDocIdBegin, subDocIdEnd);

    // [Begin, end)
    for (docid_t i = subDocIdBegin; i < subDocIdEnd; ++i)
    {
        if (!mSubModifier->RemoveDocument(i))
        {
            IE_LOG(ERROR, "failed to delete sub document [%d]", i);
            ERROR_COLLECTOR_LOG(ERROR, "failed to delete sub document [%d]", i);
        }
    }
    return mMainModifier->RemoveDocument(docId);
}

void SubDocModifier::Dump(const DirectoryPtr& directory,
                          segmentid_t segId)
{
    // TODO: when build enablePackageFile, get sub segment Directory from main segment Directory will lead to sub segment patch file included in main segment package file. because they use different inMemorySegment, lead to different storage
    DirectoryPtr subDirectory = directory->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
    assert(subDirectory);
    mSubModifier->Dump(subDirectory, segId);
    mMainModifier->Dump(directory, segId);
}

bool SubDocModifier::NeedUpdate(const NormalDocumentPtr& doc,
                                fieldid_t pkIndexField) const
{
    const AttributeDocumentPtr& attrDoc = doc->GetAttributeDocument();
    if (!attrDoc)
    {
        return false;
    }

    AttributeDocument::Iterator it = attrDoc->CreateIterator();
    fieldid_t fieldId = INVALID_FIELDID;
    while (it.HasNext())
    {
        const ConstString& fieldValue = it.Next(fieldId);
        if (fieldValue.empty())
        {
            continue;
        }
        if (fieldId != pkIndexField)
        {
            return true;
        }
    }
    return false;
}

IE_NAMESPACE_END(partition);

