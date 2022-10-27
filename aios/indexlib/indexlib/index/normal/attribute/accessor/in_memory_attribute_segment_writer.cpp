#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/dump_item_typed.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemoryAttributeSegmentWriter);

InMemoryAttributeSegmentWriter::InMemoryAttributeSegmentWriter(bool isVirtual)
    : mIsVirtual(isVirtual)
    , mEnableCounters(false)
{
}

InMemoryAttributeSegmentWriter::~InMemoryAttributeSegmentWriter() 
{
}

void InMemoryAttributeSegmentWriter::Init(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        BuildResourceMetrics* buildResourceMetrics,
        const CounterMapPtr& counterMap)
{
    assert(schema);
    mSchema = schema;
    const AttributeSchemaPtr &attrSchema = GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }
    InitAttributeWriters(attrSchema, options, buildResourceMetrics, counterMap);
}

void InMemoryAttributeSegmentWriter::InitAttributeWriters(
        const AttributeSchemaPtr &attrSchema,
        const IndexPartitionOptions& options,
        BuildResourceMetrics* buildResourceMetrics,
        const CounterMapPtr& counterMap)
{
    assert(attrSchema);
    string counterPrefix;
    if (options.IsOffline() && counterMap)
    {
        mEnableCounters = true;
        counterPrefix = "offline.updateField.";
    }
    
    AttrWriterVector attributeWriters;
    uint32_t count = attrSchema->GetAttributeCount();
    mAttrIdToAttributeWriters.resize(count);
    if (mEnableCounters)
    {
        mAttrIdToCounters.resize(count);
    }

    AttributeConfigIteratorPtr attrConfIter = attrSchema->CreateIterator(CIT_NOT_DELETE);
    for (AttributeSchema::Iterator iter = attrConfIter->Begin();
         iter != attrConfIter->End(); iter++)
    {
        AttributeConfigPtr attrConfig = *iter;
        if (mEnableCounters) 
        {
            string counterName = counterPrefix + attrConfig->GetAttrName();
            auto counter = counterMap->GetAccCounter(counterName);
            if (!counter)
            {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                        "init counter[%s] failed", counterName.c_str());
            }
            mAttrIdToCounters[attrConfig->GetAttrId()] = counter;
        }
        if (attrConfig->GetPackAttributeConfig() != NULL)
        {
            // skip attributes defined in pack attribute
            continue;
        }
        if (attrConfig->IsDisable())
        {
            IE_LOG(DEBUG, "attribute [%s] is disable", attrConfig->GetAttrName().c_str());
            continue;
        }
        AttributeWriterPtr attrWriter =
            CreateAttributeWriter(attrConfig, options, buildResourceMetrics);
        
        mAttrIdToAttributeWriters[attrConfig->GetAttrId()] = attrWriter;
    }
    
    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    mPackIdToPackAttrWriters.reserve(packAttrCount);
    mPackIdToPackFields.resize(packAttrCount);

    for (size_t i = 0; i < packAttrCount; ++i)
    {
        const PackAttributeConfigPtr& packConfig =
            attrSchema->GetPackAttributeConfig(packattrid_t(i));
        assert(packConfig);
        PackAttributeWriterPtr packAttrWriter;
        if (!packConfig->IsDisable())
        {
            packAttrWriter.reset(
                AttributeWriterFactory::GetInstance()->CreatePackAttributeWriter(
                    packConfig, options, buildResourceMetrics));
            assert(packAttrWriter);
            mPackIdToPackFields[i].reserve(packConfig->GetAttributeConfigVec().size());
        }
        mPackIdToPackAttrWriters.push_back(packAttrWriter);
    }
}

AttributeWriterPtr InMemoryAttributeSegmentWriter::CreateAttributeWriter(
        const AttributeConfigPtr& attrConfig, const IndexPartitionOptions& options,
        BuildResourceMetrics* buildResourceMetrics) const
{
    assert(attrConfig);
    AttributeWriterPtr attrWriter(
        AttributeWriterFactory::GetInstance()
        ->CreateAttributeWriter(attrConfig, options, buildResourceMetrics));
    
    assert(attrWriter);
    return attrWriter;
}


bool InMemoryAttributeSegmentWriter::AddDocument(const NormalDocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetAttributeDocument());

    const AttributeDocumentPtr& attributeDocument = doc->GetAttributeDocument();
    docid_t docId = attributeDocument->GetDocId();
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    AttributeConfigIteratorPtr attrConfigs = attrSchema->CreateIterator(ConfigIteratorType::CIT_NORMAL);
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        assert(attrConfig);
        if (attrConfig->GetPackAttributeConfig() != NULL)
        {
            continue;
        }
        fieldid_t fieldId = attrConfig->GetFieldId();
        const ConstString& field = attributeDocument->GetField(fieldId);
        mAttrIdToAttributeWriters[attrConfig->GetAttrId()]->AddField(docId, field);
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    for (packattrid_t i = 0; i < (packattrid_t)packAttrCount; ++i)
    {
        const ConstString& packField = attributeDocument->GetPackField(i);
        if (mPackIdToPackAttrWriters[i])
        {
            mPackIdToPackAttrWriters[i]->AddField(docId, packField);
        }
    }
    
    return true;
}

bool InMemoryAttributeSegmentWriter::UpdateDocument(
        docid_t docId, const NormalDocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetAttributeDocument());

    if (mIsVirtual)
    {
        IE_LOG(ERROR, "virtual attribute not support updataDocument!");
        return false;
    }

    const AttributeDocumentPtr& attrDocument = doc->GetAttributeDocument();
    UpdateFieldExtractor extractor(mSchema);
    extractor.Init(attrDocument);

    if (extractor.GetFieldCount() == 0)
    {
        IE_LOG(WARN, "get updatable attributes fail!");
        ERROR_COLLECTOR_LOG(WARN, "get updatable attributes fail!");
        return false;
    }
    UpdateAttributeFields(docId, extractor);
    return true;
}

void InMemoryAttributeSegmentWriter::UpdateAttributeFields(docid_t docId, 
        const UpdateFieldExtractor& extractor)
{
    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext())
    {
        fieldid_t fieldId = INVALID_FIELDID;
        const ConstString& field = iter.Next(fieldId);
        UpdateEncodedFieldValue(docId, fieldId, field);
    }
    
    UpdatePackAttributeFields(docId);
}

bool InMemoryAttributeSegmentWriter::UpdateEncodedFieldValue(
        docid_t docId, fieldid_t fieldId,
        const autil::ConstString& value)
{
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    const AttributeConfigPtr& attrConfig = 
        attrSchema->GetAttributeConfigByFieldId(fieldId);
    if (!attrConfig) 
    {
        IE_LOG(ERROR, "fieldId [%d] is not in Attribute!", fieldId);
        ERROR_COLLECTOR_LOG(ERROR, "fieldId [%d] is not in Attribute!", fieldId);
        return false;
    }
    if (attrConfig->IsDisable())
    {
        return true;
    }
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (packAttrConfig)
    {
        mPackIdToPackFields[packAttrConfig->GetPackAttrId()].push_back(
            make_pair(attrConfig->GetAttrId(), value));
    }
    else
    {
        auto attrId = attrConfig->GetAttrId();
        AttributeWriterPtr& writer = mAttrIdToAttributeWriters[attrId];
        if (writer)
        {
            auto ret = writer->UpdateField(docId, value);
            if (mEnableCounters && ret)
            {
                mAttrIdToCounters[attrId]->Increase(1);
            }
            return ret;
        }
        IE_LOG(ERROR, "Update field failed! attribute writer for [%s] is NULL",
               attrConfig->GetAttrName().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "Update field failed! attribute writer for [%s] is NULL",
           attrConfig->GetAttrName().c_str());
    }
    return false;
}

void InMemoryAttributeSegmentWriter::CreateInMemSegmentReaders(
        InMemorySegmentReader::String2AttrReaderMap& attrReaders)
{
    for (uint32_t i = 0; i < mAttrIdToAttributeWriters.size(); ++i)
    {
        if (mAttrIdToAttributeWriters[i])
        {
            AttributeSegmentReaderPtr attrSegReader =
                mAttrIdToAttributeWriters[i]->CreateInMemReader();
            AttributeConfigPtr attrConfig = 
                mAttrIdToAttributeWriters[i]->GetAttrConfig();
            attrReaders.insert(make_pair(attrConfig->GetAttrName(),
                                         attrSegReader));
        }
    }
    
    for (size_t i = 0; i < mPackIdToPackAttrWriters.size(); ++i)
    {
        if (mPackIdToPackAttrWriters[i])
        {
            AttributeSegmentReaderPtr attrSegReader =
                mPackIdToPackAttrWriters[i]->CreateInMemReader();
            AttributeConfigPtr attrConfig = 
                mPackIdToPackAttrWriters[i]->GetAttrConfig();
            attrReaders.insert(make_pair(attrConfig->GetAttrName(), attrSegReader));
        }
    }
}

void InMemoryAttributeSegmentWriter::CreateDumpItems(
        const file_system::DirectoryPtr& directory, 
        vector<common::DumpItem*>& dumpItems)
{
    for (AttrWriterVector::iterator it = mAttrIdToAttributeWriters.begin();
         it != mAttrIdToAttributeWriters.end(); ++it)
    {
        if (*it)
        {
            DumpItem* attrDumpItem = new AttributeDumpItem(directory, *it);
            dumpItems.push_back(attrDumpItem);
        }
    }
    
    for (PackAttrWriterVector::iterator it = mPackIdToPackAttrWriters.begin();
         it != mPackIdToPackAttrWriters.end(); ++it)
    {
        if (*it)
        {
            dumpItems.push_back(new AttributeDumpItem(directory, *it));
        }
    }
}

AttributeWriterPtr InMemoryAttributeSegmentWriter::GetAttributeWriter(
    const string& attrName)
{
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    if (!attrSchema)
    {
        return AttributeWriterPtr();
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);
    if (!attrConfig)
    {
        return AttributeWriterPtr();
    }

    attrid_t attrId = attrConfig->GetAttrId();
    assert(attrId < mAttrIdToAttributeWriters.size());
    return mAttrIdToAttributeWriters[attrId];
}

const AttributeSchemaPtr& InMemoryAttributeSegmentWriter::GetAttributeSchema() const 
{
    if (mIsVirtual)
    {
        return mSchema->GetVirtualAttributeSchema();
    }
    return mSchema->GetAttributeSchema();
}

void InMemoryAttributeSegmentWriter::GetDumpEstimateFactor(
    priority_queue<double>& factors, priority_queue<size_t>& minMemUses)
{
    for (size_t i = 0; i < mAttrIdToAttributeWriters.size(); ++i)
    {
        if (mAttrIdToAttributeWriters[i])
        {
            mAttrIdToAttributeWriters[i]->GetDumpEstimateFactor(
                factors, minMemUses);
        }
    }
    for (size_t i = 0; i < mPackIdToPackAttrWriters.size(); ++i)
    {
        if (mPackIdToPackAttrWriters[i])
        {
            mPackIdToPackAttrWriters[i]->GetDumpEstimateFactor(
                factors, minMemUses);
        }
    }
}

void InMemoryAttributeSegmentWriter::UpdatePackAttributeFields(docid_t docId)
{
    for (size_t i = 0; i < mPackIdToPackFields.size(); ++i)
    {
        if (mPackIdToPackFields[i].empty())
        {
            continue;
        }
        if (!mPackIdToPackAttrWriters[i])
        {
            continue;
        }
        bool ret = mPackIdToPackAttrWriters[i]->UpdateEncodeFields(
            docId, mPackIdToPackFields[i]);

        if (mEnableCounters && ret)
        {
            for (const auto& inPackAttr: mPackIdToPackFields[i])
            {
                mAttrIdToCounters[inPackAttr.first]->Increase(1);
            }
        }
        mPackIdToPackFields[i].clear();
    }
}

IE_NAMESPACE_END(index);

