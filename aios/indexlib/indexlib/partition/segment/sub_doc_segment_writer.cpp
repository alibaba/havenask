#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/partition/segment/in_memory_segment_modifier.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/index_define.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocSegmentWriter);

SubDocSegmentWriter::SubDocSegmentWriter(const IndexPartitionSchemaPtr& schema,
                                         const IndexPartitionOptions& options)
    : SegmentWriter(schema, options)
    , mMainJoinFieldId(INVALID_FIELDID)
    , mSubJoinFieldId(INVALID_FIELDID)
{
}

SubDocSegmentWriter::~SubDocSegmentWriter() 
{
    mMainWriter.reset();
    mSubWriter.reset();
    mBuildResourceMetrics.reset();    
}

SubDocSegmentWriter::SubDocSegmentWriter(
        const SubDocSegmentWriter& other, BuildingSegmentData& segmentData)
    : SegmentWriter(other)
    , mMainWriter(other.mMainWriter->CloneWithNewSegmentData(segmentData))
    , mSubWriter(other.mSubWriter->CloneWithNewSegmentData(segmentData))
    , mMainJoinFieldId(other.mMainJoinFieldId)
    , mSubJoinFieldId(other.mSubJoinFieldId)
    , mMainJoinFieldConvertor(other.mMainJoinFieldConvertor)
    , mSubJoinFieldConvertor(other.mSubJoinFieldConvertor)
    , mMainModifier(other.mMainModifier)
{
}

void SubDocSegmentWriter::Init(
        const index_base::SegmentData& segmentData,
        const SegmentMetricsPtr& metrics,
        const CounterMapPtr& counterMap,
        const PluginManagerPtr& pluginManager,
        const PartitionSegmentIteratorPtr& partSegIter)
{
    mBuildResourceMetrics.reset(new BuildResourceMetrics());
    mBuildResourceMetrics->Init();
    mCounterMap = counterMap;
    mSegmentMetrics = metrics;
    mMainWriter = CreateSingleSegmentWriter(
            mSchema, mOptions, segmentData, metrics, pluginManager, partSegIter, false);

    SegmentDataPtr subSegmentData = segmentData.GetSubSegmentData();
    PartitionSegmentIteratorPtr subPartSegIter;
    if (partSegIter) {
        subPartSegIter.reset(partSegIter->CreateSubPartitionSegmentItertor());
    }
    assert(subSegmentData);
    mSubWriter = CreateSingleSegmentWriter(
            mSchema->GetSubIndexPartitionSchema(), mOptions,
            *subSegmentData, metrics, pluginManager, subPartSegIter, true);

    mMainModifier = mMainWriter->GetInMemorySegmentModifier();
    InitFieldIds(mSchema);
}

SubDocSegmentWriter* SubDocSegmentWriter::CloneWithNewSegmentData(
        BuildingSegmentData& segmentData) const
{
    return new SubDocSegmentWriter(*this, segmentData);
}

SingleSegmentWriterPtr SubDocSegmentWriter::CreateSingleSegmentWriter(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const SegmentData& segmentData,
        const index_base::SegmentMetricsPtr& metrics,
        const PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& partSegIter,
        bool isSub)
{
    assert(schema);
    assert(mBuildResourceMetrics);
    SingleSegmentWriterPtr segmentWriter(new SingleSegmentWriter(schema, options, isSub));
    segmentWriter->Init(segmentData, metrics, mBuildResourceMetrics,
                        mCounterMap, pluginManager, partSegIter);
    return segmentWriter;
}

void SubDocSegmentWriter::DedupSubDoc(const NormalDocumentPtr& doc) const
{
    //TODO: move to processor
    set<string> pkSet;
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for(int32_t i = (int32_t)subDocs.size() - 1; i >= 0; --i)
    {
        const NormalDocumentPtr& subDocument = subDocs[i];
        assert(subDocument);
        if (IsSubDocDuplicated(subDocument, pkSet))
        {
            IE_LOG(WARN, "duplicate sub doc, mainPK[%s], subPK[%s]",
                   doc->HasPrimaryKey() ?
                   doc->GetIndexDocument()->GetPrimaryKey().c_str() : 
                   string("pk empty").c_str(),
                   subDocument->HasPrimaryKey() ?
                   subDocument->GetIndexDocument()->GetPrimaryKey().c_str() : 
                   string("sub pk empty").c_str());
            doc->RemoveSubDocument(i);
        }
        else
        {
            const string& subPrimaryKey =
                subDocument->GetIndexDocument()->GetPrimaryKey();
            pkSet.insert(subPrimaryKey);
        }
    }
}

bool SubDocSegmentWriter::AddDocument(const document::DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    DedupSubDoc(doc);

    AddJoinFieldToDocument(doc);

    //step 1: add main doc, not set main join
    if (!mMainWriter->AddDocument(document))
    {
        IE_LOG(WARN, "add main document fail! main doc pk[%s]",
               doc->HasPrimaryKey() ?
               doc->GetIndexDocument()->GetPrimaryKey().c_str() :
               string("pk empty").c_str());
        ERROR_COLLECTOR_LOG(WARN, "add main document fail! main doc pk[%s]", 
               doc->HasPrimaryKey() ?
               doc->GetIndexDocument()->GetPrimaryKey().c_str() :
               string("pk empty").c_str());
        return false;
    }

    //step 2: add sub doc, set sub join
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for(size_t i = 0; i < subDocs.size(); i++)
    {
        const NormalDocumentPtr& subDoc = subDocs[i];
        if (!mSubWriter->AddDocument(subDoc))
        {
            IE_LOG(WARN, "add sub document fail!"
                   "main doc pk[%s], sub doc pk[%s]", 
                   doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                   subDoc->HasPrimaryKey() ?
                   subDoc->GetIndexDocument()->GetPrimaryKey().c_str() :
                   string("pk empty").c_str());
            ERROR_COLLECTOR_LOG(WARN, "add sub document fail!"
                   "main doc pk[%s], sub doc pk[%s]", 
                   doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                   subDoc->HasPrimaryKey() ?
                   subDoc->GetIndexDocument()->GetPrimaryKey().c_str() :
                   string("pk empty").c_str());
        }
    }

    //step 3: update main join
    UpdateMainDocJoinValue();

    return true;
}

void SubDocSegmentWriter::UpdateMainDocJoinValue()
{
    const SegmentInfoPtr& mainSegmentInfo = mMainWriter->GetSegmentInfo();
    docid_t updateMainDocId = (docid_t)mainSegmentInfo->docCount - 1;

    const SegmentInfoPtr& subSegmentInfo = mSubWriter->GetSegmentInfo();
    string updateJoinValueStr = StringUtil::toString(subSegmentInfo->docCount);

    string mainConvertedJoinValue = 
        mMainJoinFieldConvertor->Encode(updateJoinValueStr);

    mMainModifier->UpdateEncodedFieldValue(updateMainDocId, mMainJoinFieldId,
            ConstString(mainConvertedJoinValue));
}

void SubDocSegmentWriter::CollectSegmentMetrics()
{
    mMainWriter->CollectSegmentMetrics();
    mSubWriter->CollectSegmentMetrics();
}

void SubDocSegmentWriter::EndSegment()
{
    mMainWriter->EndSegment();
}

void SubDocSegmentWriter::CreateDumpItems(const file_system::DirectoryPtr& directory,
        vector<common::DumpItem*>& dumpItems)
{
    mMainWriter->CreateDumpItems(directory, dumpItems);
}

index::InMemorySegmentReaderPtr SubDocSegmentWriter::CreateInMemSegmentReader()
{
    return mMainWriter->CreateInMemSegmentReader();
}

bool SubDocSegmentWriter::IsDirty() const
{
    return mMainWriter->IsDirty() || mSubWriter->IsDirty();
}
    
void SubDocSegmentWriter::AddJoinFieldToDocument(const NormalDocumentPtr& doc) const
{
    string mainJoinValueStr = StringUtil::toString(INVALID_DOCID);
    ConstString mainConvertedJoinValue = mMainJoinFieldConvertor->Encode(
            ConstString(mainJoinValueStr), doc->GetPool());
    if (!doc->GetAttributeDocument())
    {
        //TODO: no pool
        doc->SetAttributeDocument(AttributeDocumentPtr(new AttributeDocument));
    }
    const AttributeDocumentPtr& mainAttrDocument = doc->GetAttributeDocument();
    mainAttrDocument->SetField(mMainJoinFieldId, mainConvertedJoinValue);
    AddJoinFieldToSubDocuments(doc);
}

void SubDocSegmentWriter::InitFieldIds(
        const IndexPartitionSchemaPtr& mainSchema)
{
    const FieldSchemaPtr& mainFieldSchema = mainSchema->GetFieldSchema();
    const IndexPartitionSchemaPtr& subSchema =
        mainSchema->GetSubIndexPartitionSchema();
    const FieldSchemaPtr& subFieldSchema = subSchema->GetFieldSchema();

    // init join field ids
    mMainJoinFieldConvertor = CreateConvertor(
            MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, mainFieldSchema);
    mMainJoinFieldId = mainFieldSchema->GetFieldId(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    mSubJoinFieldConvertor = CreateConvertor(
            SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, subFieldSchema);
    mSubJoinFieldId = subFieldSchema->GetFieldId(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
}

void SubDocSegmentWriter::AddJoinFieldToSubDocuments(const NormalDocumentPtr& doc) const
{
    //TODO: optimize with encoder
    const SegmentInfoPtr& mainSegmentInfo = mMainWriter->GetSegmentInfo();
    string subJoinValueStr = StringUtil::toString(mainSegmentInfo->docCount);

    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); ++i)
    {
        const NormalDocumentPtr& subDocument = subDocs[i];
        assert(subDocument);

        if (!subDocument->GetAttributeDocument())
        {
            subDocument->SetAttributeDocument(
                    AttributeDocumentPtr(new AttributeDocument));
        }

        const AttributeDocumentPtr& subAttrDocument = 
            subDocument->GetAttributeDocument();
        ConstString subConvertedJoinValue = mSubJoinFieldConvertor->Encode(
                ConstString(subJoinValueStr), subDocument->GetPool());
        subAttrDocument->SetField(mSubJoinFieldId, subConvertedJoinValue);
    }
}

AttributeConvertorPtr SubDocSegmentWriter::CreateConvertor(
        const string& joinFieldName,
        const FieldSchemaPtr& fieldSchema)
{
    FieldConfigPtr joinFieldConfig = 
        fieldSchema->GetFieldConfig(joinFieldName);
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(joinFieldConfig));
    assert(convertor);
    return convertor;
}

bool SubDocSegmentWriter::IsSubDocDuplicated(
        const NormalDocumentPtr& subDoc, const set<string>& pkSet) const
{
    if (!subDoc->HasPrimaryKey())
    {
        return true;
    }

    const string& subPrimaryKey =
        subDoc->GetIndexDocument()->GetPrimaryKey();
    if (pkSet.count(subPrimaryKey) > 0)
    {
        return true;
    }
    return false;
}

const SegmentInfoPtr& SubDocSegmentWriter::GetSegmentInfo() const
{
    return mMainWriter->GetSegmentInfo();
}

size_t SubDocSegmentWriter::EstimateInitMemUse(
        const index_base::SegmentMetricsPtr& metrics,
        const util::QuotaControlPtr& buildMemoryQuotaControler,
        const plugin::PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& partSegIter)
{
    size_t memUse = 0;
    SingleSegmentWriterPtr segmentWriter(new SingleSegmentWriter(mSchema, mOptions));
    memUse += segmentWriter->EstimateInitMemUse(metrics,
            buildMemoryQuotaControler, pluginManager, partSegIter);

    index_base::PartitionSegmentIteratorPtr subSegIter;
    if (partSegIter) {
        subSegIter.reset(partSegIter->CreateSubPartitionSegmentItertor());
    }
    SingleSegmentWriterPtr subSegmentWriter(new SingleSegmentWriter(
                    mSchema->GetSubIndexPartitionSchema(), mOptions));
    memUse += subSegmentWriter->EstimateInitMemUse(metrics,
            buildMemoryQuotaControler, pluginManager, subSegIter);
    return memUse;
}

IE_NAMESPACE_END(partition);

