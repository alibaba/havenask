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
#include "indexlib/partition/offline_partition_reader.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflinePartitionReader);

OfflinePartitionReader::OfflinePartitionReader(const IndexPartitionOptions& options,
                                               const IndexPartitionSchemaPtr& schema,
                                               const SearchCachePartitionWrapperPtr& searchCache)
    : OnlinePartitionReader(options, schema, searchCache, NULL)
{
}

void OfflinePartitionReader::Open(const index_base::PartitionDataPtr& partitionData)
{
    IE_LOG(INFO, "OfflinePartitionReader open begin");
    mPartitionData = partitionData;
    InitPartitionVersion(partitionData, INVALID_SEGMENTID);

    try {
        InitDeletionMapReader();
        if (mOptions.GetOfflineConfig().readerConfig.loadIndex) {
            InitIndexReaders(partitionData);
            InitKKVReader();
            InitKVReader();
            InitAttributeReaders(mOptions.GetOfflineConfig().readerConfig.readPreference,
                                 /*needPackAttributeReaders=*/false, /*hint reader*/ nullptr);
            InitSummaryReader();
            InitSourceReader();
        }
    } catch (const FileIOException& ioe) {
        // do not catch FileIOException, just throw it out to searcher
        // searcher will decide how to handle this exception
        IE_LOG(ERROR, "caught FileIOException when opening index partition");
        throw;
    } catch (const ExceptionBase& e) {
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
    if (!mSummaryReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitSummaryReader);
    }
    return OnlinePartitionReader::GetSummaryReader();
}

const SourceReaderPtr& OfflinePartitionReader::GetSourceReader() const
{
    ScopedLock lock(mLock);
    if (!mSourceReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitSourceReader);
    }
    return OnlinePartitionReader::GetSourceReader();
}

const AttributeReaderPtr& OfflinePartitionReader::GetAttributeReader(const string& field) const
{
    ScopedLock lock(mLock);
    const AttributeReaderPtr& attrReader = OnlinePartitionReader::GetAttributeReader(field);
    if (attrReader || mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        return attrReader;
    }
    return CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitOneFieldAttributeReader, field);
}

const PackAttributeReaderPtr& OfflinePartitionReader::GetPackAttributeReader(const string& packAttrName) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetPackAttributeReader!");
    return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
}

const PackAttributeReaderPtr& OfflinePartitionReader::GetPackAttributeReader(packattrid_t packAttrId) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetPackAttributeReader!");
    return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
}

InvertedIndexReaderPtr OfflinePartitionReader::GetInvertedIndexReader() const
{
    if (mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        return OnlinePartitionReader::GetInvertedIndexReader();
    }
    INDEXLIB_FATAL_ERROR(UnSupported, "not support GetInvertedIndexReader!");
    return InvertedIndexReaderPtr();
}

const InvertedIndexReaderPtr& OfflinePartitionReader::GetInvertedIndexReader(const string& indexName) const
{
    const InvertedIndexReaderPtr& indexReader = OnlinePartitionReader::GetInvertedIndexReader(indexName);
    if (indexReader || mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        return indexReader;
    }
    return CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitOneIndexReader, indexName);
}

const InvertedIndexReaderPtr& OfflinePartitionReader::GetInvertedIndexReader(indexid_t indexId) const
{
    const InvertedIndexReaderPtr& indexReader = OnlinePartitionReader::GetInvertedIndexReader(indexId);
    if (indexReader || mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        return indexReader;
    }

    IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    if (!indexSchema) {
        return RET_EMPTY_INDEX_READER;
    }

    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexId);
    if (!indexConfig) {
        return RET_EMPTY_INDEX_READER;
    }
    return CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitOneIndexReader, indexConfig->GetIndexName());
}

const PrimaryKeyIndexReaderPtr& OfflinePartitionReader::GetPrimaryKeyReader() const
{
    ScopedLock lock(mLock);
    if (!mPrimaryKeyIndexReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitPrimaryKeyIndexReader, mPartitionData);
    }
    return OnlinePartitionReader::GetPrimaryKeyReader();
}

const KKVReaderPtr& OfflinePartitionReader::GetKKVReader(const string& regionName) const
{
    ScopedLock lock(mLock);
    if (!mKKVReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitKKVReader);
    }
    return OnlinePartitionReader::GetKKVReader(regionName);
}

const KKVReaderPtr& OfflinePartitionReader::GetKKVReader(regionid_t regionId) const
{
    ScopedLock lock(mLock);
    if (!mKKVReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitKKVReader);
    }
    return OnlinePartitionReader::GetKKVReader(regionId);
}

const KVReaderPtr& OfflinePartitionReader::GetKVReader(const string& regionName) const
{
    ScopedLock lock(mLock);
    if (!mKVReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitKVReader);
    }
    return OnlinePartitionReader::GetKVReader(regionName);
}

const KVReaderPtr& OfflinePartitionReader::GetKVReader(regionid_t regionId) const
{
    ScopedLock lock(mLock);
    if (!mKVReader && !mOptions.GetOfflineConfig().readerConfig.loadIndex) {
        CALL_NOT_CONST_FUNC(OfflinePartitionReader, InitKVReader);
    }
    return OnlinePartitionReader::GetKVReader(regionId);
}

void OfflinePartitionReader::InitAttributeReaders(ReadPreference readPreference, bool needPackAttrReader,
                                                  const OnlinePartitionReader* hintPartReader)
{
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv) {
        return;
    }
    AttributeMetrics* attrMetrics = NULL;
    if (mOnlinePartMetrics) {
        attrMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    }

    IE_LOG(INFO, "Init Attribute container begin");
    mAttrReaderContainer.reset(new indexlib::index::AttributeReaderContainer(mSchema));
    mAttrReaderContainer->Init(mPartitionData, attrMetrics, readPreference, needPackAttrReader,
                               mOptions.GetOnlineConfig().GetInitReaderThreadCount(), nullptr);

    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }
    auto configIter = attrSchema->CreateIterator();
    for (auto iter = configIter->Begin(); iter != configIter->End(); iter++) {
        auto attrConfig = *iter;
        const AttributeReaderPtr& reader = mAttrReaderContainer->GetAttributeReader(attrConfig->GetAttrName());
        if (readPreference == ReadPreference::RP_RANDOM_PREFER && reader && attrConfig->IsAttributeUpdatable()) {
            PatchLoader::LoadAttributePatch(*reader, attrConfig, mPartitionData);
        }
    }
    IE_LOG(INFO, "Init Attribute container end");
}

const AttributeReaderPtr& OfflinePartitionReader::InitOneFieldAttributeReader(const string& field)
{
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(field);
    if (!attrConfig) {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
    }

    IE_LOG(INFO, "Begin on-demand init attribute reader [%s]", field.c_str());
    if (!mAttrReaderContainer) {
        mAttrReaderContainer.reset(new indexlib::index::AttributeReaderContainer(mSchema));
    }

    auto readPreference = mOptions.GetOfflineConfig().readerConfig.readPreference;
    mAttrReaderContainer->InitAttributeReader(mPartitionData, readPreference, field, nullptr);
    const AttributeReaderPtr& reader = mAttrReaderContainer->GetAttributeReader(field);
    if (readPreference == ReadPreference::RP_RANDOM_PREFER && reader && attrConfig->IsAttributeUpdatable()) {
        PatchLoader::LoadAttributePatch(*reader, attrConfig, mPartitionData);
    }

    IE_LOG(INFO, "End on-demand init attribute reader [%s]", field.c_str());
    return reader;
}

const InvertedIndexReaderPtr& OfflinePartitionReader::InitOneIndexReader(const string& indexName)
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    if (!indexSchema) {
        return RET_EMPTY_INDEX_READER;
    }

    const IndexConfigPtr& indexConfig = indexSchema->GetIndexConfig(indexName);
    if (!indexConfig) {
        return RET_EMPTY_INDEX_READER;
    }

    IE_LOG(INFO, "Begin on-demand init index reader [%s]", indexName.c_str());
    InvertedIndexReaderPtr indexReader = InitSingleIndexReader(indexConfig, mPartitionData);
    if (!indexReader) {
        return RET_EMPTY_INDEX_READER;
    }

    if (!mIndexReader) {
        mIndexReader.reset(new index::legacy::MultiFieldIndexReader(NULL));
    }
    mIndexReader->AddIndexReader(indexConfig, indexReader);
    IE_LOG(INFO, "End on-demand init index reader [%s]", indexName.c_str());
    return mIndexReader->GetInvertedIndexReader(indexName);
}
}} // namespace indexlib::partition
