#include "indexlib/partition/offline_partition_reader.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"


using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartitionReader);

OfflinePartitionReader::OfflinePartitionReader(const IndexPartitionOptions& options,
                                               const IndexPartitionSchemaPtr& schema,
                                               const SearchCachePartitionWrapperPtr& searchCache)
    : OnlinePartitionReader(options, schema, searchCache, NULL)
{}


void OfflinePartitionReader::Open(const index_base::PartitionDataPtr& partitionData)
{
    IE_LOG(INFO, "OfflinePartitionReader open begin");
    mPartitionData = partitionData;
    InitPartitionVersion(partitionData, INVALID_SEGMENTID);

    try
    {
        InitDeletionMapReader();
        if (mOptions.GetOfflineConfig().readerConfig.loadIndex)
        {
            InitIndexReaders(partitionData);
            InitAttributeReaders(mOptions.GetOfflineConfig().readerConfig.lazyLoadAttribute,
                                 false);
            InitSummaryReader();
        }
    }
    catch(const FileIOException& ioe)
    {
        // do not catch FileIOException, just throw it out to searcher
        // searcher will decide how to handle this exception
        IE_LOG(ERROR, "caught FileIOException when opening index partition");
        throw;
    }
    catch(const ExceptionBase& e)
    {
        stringstream ss;
        ss << "FAIL to Load latest version: " << e.ToString();
        IE_LOG(WARN, "%s", ss.str().data());
        throw;
    }
    IE_LOG(INFO, "OfflinePartitionReader open end");
}

const SummaryReaderPtr& OfflinePartitionReader::GetSummaryReader() const
{
    ScopedLock lock(mLock);
    if (!mSummaryReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex)
    {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitSummaryReader);
    }
    return OnlinePartitionReader::GetSummaryReader();
}

const AttributeReaderPtr& OfflinePartitionReader::GetAttributeReader(
        const string& field) const
{
    ScopedLock lock(mLock);
    const AttributeReaderPtr& attrReader = OnlinePartitionReader::GetAttributeReader(field);
    if (attrReader || mOptions.GetOfflineConfig().readerConfig.loadIndex)
    {
        return attrReader;
    }
    return CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitOneFieldAttributeReader, field);
}

const PackAttributeReaderPtr& OfflinePartitionReader::GetPackAttributeReader(
        const string& packAttrName) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetPackAttributeReader!");
    return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
}

const PackAttributeReaderPtr& OfflinePartitionReader::GetPackAttributeReader(
        packattrid_t packAttrId) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetPackAttributeReader!");
    return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
}

IndexReaderPtr OfflinePartitionReader::GetIndexReader() const
{
    if (mOptions.GetOfflineConfig().readerConfig.loadIndex)
    {
        return OnlinePartitionReader::GetIndexReader();
    }
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetIndexReader!");
    return IndexReaderPtr();
}

const IndexReaderPtr& OfflinePartitionReader::GetIndexReader(
        const string& indexName) const
{
    if (mOptions.GetOfflineConfig().readerConfig.loadIndex)
    {
        return OnlinePartitionReader::GetIndexReader(indexName);
    }
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetIndexReader!");
    return RET_EMPTY_INDEX_READER;
}

const IndexReaderPtr& OfflinePartitionReader::GetIndexReader(
        indexid_t indexId) const
{
    if (mOptions.GetOfflineConfig().readerConfig.loadIndex)
    {
        return OnlinePartitionReader::GetIndexReader(indexId);
    }
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetIndexReader!");
    return RET_EMPTY_INDEX_READER;
}

const PrimaryKeyIndexReaderPtr& OfflinePartitionReader::GetPrimaryKeyReader() const
{
    ScopedLock lock(mLock);
    if (!mPrimaryKeyIndexReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex)
    {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitPrimaryKeyIndexReader, mPartitionData);
    }
    return OnlinePartitionReader::GetPrimaryKeyReader();
}

const AttributeReaderPtr& OfflinePartitionReader::InitOneFieldAttributeReader(const string& field)
{
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(field);
    if (!attrConfig)
    {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }

    IE_LOG(INFO, "Begin on-demand init attribute reader [%s]", field.c_str());
    if (!mAttrReaderContainer)
    {
        mAttrReaderContainer.reset(new IE_NAMESPACE(index)::AttributeReaderContainer(mSchema));
    }
    mAttrReaderContainer->InitAttributeReader(
        mPartitionData, mOptions.GetOfflineConfig().readerConfig.lazyLoadAttribute, field);
    const AttributeReaderPtr& reader = mAttrReaderContainer->GetAttributeReader(field);
    if (reader && attrConfig->IsAttributeUpdatable())
    {
        PatchLoader::LoadAttributePatch(*reader, attrConfig, mPartitionData);
    }
    IE_LOG(INFO, "End on-demand init attribute reader [%s]", field.c_str());
    return reader;
}

IE_NAMESPACE_END(partition);
