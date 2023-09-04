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
#include "indexlib/partition/index_application.h"

#include "autil/HashAlgorithm.h"
#include "autil/LoopThread.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/table/normal_table/Common.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexApplication);

IndexApplication::IndexApplication()
{
    mBackgroundUpdateCacheSnapshotTimeUs =
        autil::EnvUtil::getEnv<int64_t>("background_update_cache_snapshot_time_us", 10 * 1000); // 10ms
}

IndexApplication::~IndexApplication()
{
    mUpdateExpiredSnapshotThread.reset();
    Close();
}

void IndexApplication::Close()
{
    autil::ScopedWriteLock wrlock(mSnapshotLock);
    mSnapshotCacheTimeUs = -1;
    mLastSnapshotCreateTime = -1;
    mLastPartitionReaderSnapshot.reset();
}

bool IndexApplication::Init(const vector<IndexPartitionPtr>& indexPartitions, const JoinRelationMap& joinRelations)
{
    if (_hasTablet) {
        IE_LOG(ERROR, "IndexPartition should't be create after tablet");
        return false;
    }
    if (!PrepareBackgroundSnapshotReaderThread()) {
        return false;
    }
    mReaderUpdater.reset(new TableReaderContainerUpdater(std::bind(&IndexApplication::updateCallBack, this)));
    if (!AddIndexPartitions(indexPartitions)) {
        return false;
    }
    mReaderContainer.Init(mTableName2PartitionIdMap);
    return mReaderUpdater->Init(mIndexPartitionVec, joinRelations, &mReaderContainer);
}

bool IndexApplication::Init(const IndexPartitionMap& indexPartitions, const JoinRelationMap& joinRelations)
{
    if (!PrepareBackgroundSnapshotReaderThread()) {
        return false;
    }
    vector<IndexPartitionPtr> partitionVec;
    IndexPartitionMap::const_iterator iter = indexPartitions.begin();
    for (; iter != indexPartitions.end(); iter++) {
        partitionVec.push_back(iter->second);
    }
    return Init(partitionVec, joinRelations);
}

bool IndexApplication::Init(const vector<partition::IndexPartitionPtr>& indexPartitions)
{
    if (_hasTablet) {
        IE_LOG(ERROR, "IndexPartition should't be create after tablet");
        return false;
    }
    if (!PrepareBackgroundSnapshotReaderThread()) {
        return false;
    }
    mReaderUpdater.reset(new TableReaderContainerUpdater());
    mIndexPartitionVec.clear();
    for (size_t i = 0; i < indexPartitions.size(); i++) {
        mIndexPartitionVec.push_back(indexPartitions[i]);
    }
    if (!AddIndexPartitionsFromVec()) {
        return false;
    }
    mReaderContainer.Init(mTableName2PartitionIdMap);
    for (size_t i = 0; i < indexPartitions.size(); i++) {
        mReaderContainer.UpdateReader(mIndexPartitionVec[i]->GetSchema()->GetSchemaName(),
                                      mIndexPartitionVec[i]->GetReader());
    }
    return true;
}

bool IndexApplication::PrepareBackgroundSnapshotReaderThread()
{
    if (mUpdateExpiredSnapshotThread) {
        return true;
    }
    mUpdateExpiredSnapshotThread =
        autil::LoopThread::createLoopThread(std::bind(&IndexApplication::BackgroundUpdateExpiredSnapshotReader, this),
                                            mBackgroundUpdateCacheSnapshotTimeUs);
    if (!mUpdateExpiredSnapshotThread) {
        IE_LOG(ERROR, "create update cache snapshot thread failed")
        return false;
    }
    return true;
}

bool IndexApplication::InitByTablet(const TabletMap& tabletMap)
{
    if (!PrepareBackgroundSnapshotReaderThread()) {
        return false;
    }
    _tabletReaderTypeBaseIdx = mReaderTypeVec.size();
    _tablets.clear();
    for (auto tabletPair : tabletMap) {
        _tablets.push_back(tabletPair.second);
    }
    if (!AddTablets()) {
        _hasTablet = false;
        _tablets.clear();
        return false;
    }
    _hasTablet = true;
    return true;
}

bool IndexApplication::AddIndexPartitions(const vector<IndexPartitionPtr>& indexPartitions)
{
    mIndexPartitionVec.assign(indexPartitions.begin(), indexPartitions.end());
    return AddIndexPartitionsFromVec();
}

bool IndexApplication::AddIndexPartitionsFromVec()
{
    uint32_t id = 0;
    for (size_t i = 0; i < mIndexPartitionVec.size(); i++) {
        const IndexPartitionPtr& indexPart = mIndexPartitionVec[i];
        const IndexPartitionSchemaPtr& schema = indexPart->GetSchema();
        assert(schema);
        const string& tableName = schema->GetSchemaName();
        if (mTableName2PartitionIdMap.count(tableName) != 0) {
            IE_LOG(ERROR, "table [%s] already exist", tableName.c_str());
            return false;
        }
        mTableName2PartitionIdMap[tableName] = i;
        if (!AddIndexPartition(schema, id++)) {
            return false;
        }
        mReaderTypeVec.push_back(READER_PRIMARY_MAIN_TYPE);
        const IndexPartitionSchemaPtr& subPartSchema = schema->GetSubIndexPartitionSchema();
        if (subPartSchema) {
            if (!AddIndexPartition(subPartSchema, id++)) {
                return false;
            }
            mReaderTypeVec.push_back(READER_PRIMARY_SUB_TYPE);
        }
    }
    return true;
}

bool IndexApplication::AddTablets()
{
    // part idx array = [index parttion idx array] + [tablet idx array]
    auto partBaseIdx = mIndexPartitionVec.size();
    auto readerBaseIdx = _tabletReaderTypeBaseIdx;
    for (size_t i = 0; i < _tablets.size(); ++i) {
        const std::shared_ptr<indexlibv2::framework::ITablet>& tablet = _tablets[i];
        const auto& schema = tablet->GetTabletSchema();
        assert(schema);
        const string& tableName = schema->GetTableName();
        if (mTableName2PartitionIdMap.count(tableName) != 0) {
            IE_LOG(ERROR, "table [%s] already exist", tableName.c_str());
            return false;
        }
        mTableName2PartitionIdMap[tableName] = i + partBaseIdx;
        if (!AddTablet(schema, i + readerBaseIdx)) {
            return false;
        }
        mReaderTypeVec.push_back(READER_PRIMARY_MAIN_TYPE);
    }
    return true;
}

bool IndexApplication::AddTablet(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, uint32_t id)
{
    const auto& tableName = schema->GetTableName();
    auto addAttributes = [this, tableName, schema, id]() {
        auto indexConfigs = schema->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR);
        for (const auto& indexConfig : indexConfigs) {
            const auto& attrConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
            if (!attrConfig) {
                IE_LOG(ERROR, "tableName [%s] cast attribute config [%s] failed", tableName.c_str(),
                       indexConfig->GetIndexName().c_str());
                return false;
            }
            if (!attrConfig->IsNormal()) {
                continue;
            }
            const auto& attrName = attrConfig->GetIndexName();
            if (!mAttrName2IdMap.Exist(tableName, attrName)) {
                mAttrName2IdMap.Insert(tableName, attrName, id);
            } else {
                IE_LOG(ERROR, "duplicate table name[%s] and attribute name[%s] in different table", tableName.c_str(),
                       attrName.c_str());
                return false;
            }
        }
        return true;
    };
    auto addPackAttributes = [this, tableName, schema, id]() {
        auto indexConfigs = schema->GetIndexConfigs(indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
        for (const auto& indexConfig : indexConfigs) {
            const auto& packAttrConfig = std::dynamic_pointer_cast<indexlibv2::index::PackAttributeConfig>(indexConfig);
            if (!packAttrConfig) {
                IE_LOG(ERROR, "tableName [%s] cast pack attribute [%s] failed", tableName.c_str(),
                       indexConfig->GetIndexName().c_str());
                return false;
            }
            const auto& packName = packAttrConfig->GetPackName();
            if (!mPackAttrName2IdMap.Exist(tableName, packName)) {
                mPackAttrName2IdMap.Insert(tableName, packName, id);
            } else {
                IE_LOG(ERROR,
                       "duplicate table name [%s] pack attribute name[%s]"
                       " in different table",
                       tableName.c_str(), packName.c_str());
                return false;
            }
        }
        return true;
    };
    if (!addAttributes()) {
        IE_LOG(ERROR, "add attribute failed");
        return false;
    }
    if (!addPackAttributes()) {
        IE_LOG(ERROR, "add pack attribute failed");
        return false;
    }
    auto indexConfigs = schema->GetIndexConfigs(index::GENERAL_INVERTED_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig);
        if (!invertedIndexConfig->IsNormal()) {
            continue;
        }
        auto invertedIndexType = invertedIndexConfig->GetInvertedIndexType();
        if (invertedIndexType == it_primarykey64 || invertedIndexType == it_primarykey128) {
            continue;
        }
        const string& indexName = invertedIndexConfig->GetIndexName();
        if (!mIndexName2IdMap.Exist(tableName, indexName)) {
            mIndexName2IdMap.Insert(tableName, indexName, id);
        } else {
            IE_LOG(ERROR,
                   "duplicate table name [%s] index name[%s]"
                   " in different table",
                   tableName.c_str(), indexName.c_str());
            return false;
        }
    }
    auto pkConfig = schema->GetPrimaryKeyIndexConfig();
    if (pkConfig) {
        const auto& indexName = pkConfig->GetIndexName();
        if (!mIndexName2IdMap.Exist(tableName, indexName)) {
            mIndexName2IdMap.Insert(tableName, indexName, id);
        } else {
            IE_LOG(ERROR,
                   "duplicate table name [%s] index name[%s]"
                   " in different table",
                   tableName.c_str(), indexName.c_str());
            return false;
        }
    }
    return true;
}

bool IndexApplication::AddIndexPartition(const IndexPartitionSchemaPtr& schema, uint32_t id)
{
#define add_one_schema(attrSchema)                                                                                     \
    do {                                                                                                               \
        AttributeSchema::Iterator attrIter = attrSchema->Begin();                                                      \
        for (; attrIter != attrSchema->End(); attrIter++) {                                                            \
            const AttributeConfigPtr& attrConfig = *attrIter;                                                          \
            if (!attrConfig->IsNormal()) {                                                                             \
                continue;                                                                                              \
            }                                                                                                          \
            const string& attrName = attrConfig->GetAttrName();                                                        \
            if (!mAttrName2IdMap.Exist(tableName, attrName)) {                                                         \
                mAttrName2IdMap.Insert(tableName, attrName, id);                                                       \
            } else {                                                                                                   \
                IE_LOG(ERROR,                                                                                          \
                       "duplicate table name [%s] and attribute name[%s] "                                             \
                       "in different table",                                                                           \
                       tableName.c_str(), attrName.c_str());                                                           \
                return false;                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
        for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); ++i) {                                             \
            const PackAttributeConfigPtr& packAttrConfig = attrSchema->GetPackAttributeConfig(i);                      \
            const string& packName = packAttrConfig->GetPackName();                                                    \
            if (!mPackAttrName2IdMap.Exist(tableName, packName)) {                                                     \
                mPackAttrName2IdMap.Insert(tableName, packName, id);                                                   \
            } else {                                                                                                   \
                IE_LOG(ERROR,                                                                                          \
                       "duplicate table name [%s] pack attribute name[%s]"                                             \
                       " in different table",                                                                          \
                       tableName.c_str(), packName.c_str());                                                           \
                return false;                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

    const string& tableName = schema->GetSchemaName();
    const AttributeSchemaPtr& attributeSchema = schema->GetAttributeSchema();
    if (attributeSchema) {
        add_one_schema(attributeSchema);
    }
    const AttributeSchemaPtr& virtualAttrSchema = schema->GetVirtualAttributeSchema();
    if (virtualAttrSchema) {
        add_one_schema(virtualAttrSchema);
    }
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    if (indexSchema) {
        IndexSchema::Iterator indexIter = indexSchema->Begin();
        for (; indexIter != indexSchema->End(); indexIter++) {
            const IndexConfigPtr& indexConfig = *indexIter;
            if (!indexConfig->IsNormal()) {
                continue;
            }
            const string& indexName = indexConfig->GetIndexName();
            if (!mIndexName2IdMap.Exist(tableName, indexName)) {
                mIndexName2IdMap.Insert(tableName, indexName, id);
            } else {
                IE_LOG(ERROR,
                       "duplicate table name [%s] index name[%s]"
                       " in different table",
                       tableName.c_str(), indexName.c_str());
                return false;
            }
        }
    }
    return true;
}

bool IndexApplication::IsLatestSnapshotReader(const PartitionReaderSnapshotPtr& snapshotReader) const
{
    assert(snapshotReader);
    uint32_t readerId = 0;
    for (uint32_t i = 0; i < mReaderContainer.Size(); i++) {
        const IndexPartitionReaderPtr& snapshotPartReader = snapshotReader->GetIndexPartitionReader(readerId);
        if (unlikely(mReaderContainer.GetReader(i)->GetPartitionVersionHashId() !=
                     snapshotPartReader->GetPartitionVersionHashId())) {
            return false;
        }
        ++readerId;
        if (readerId < mReaderTypeVec.size() && mReaderTypeVec[readerId] == READER_PRIMARY_SUB_TYPE) {
            // ignore sub reader, only check main reader
            // because sub reader has same PartitionVersion with main reader
            ++readerId;
            continue;
        }
    }
    return true;
}

void IndexApplication::GetExpireReaderNames(const PartitionReaderSnapshotPtr& snapshotReader,
                                            std::vector<std::string>& tables) const
{
    assert(snapshotReader);
    uint32_t readerId = 0;
    for (uint32_t i = 0; i < mReaderContainer.Size(); i++) {
        const IndexPartitionReaderPtr& snapshotPartReader = snapshotReader->GetIndexPartitionReader(readerId);
        if (unlikely(mReaderContainer.GetReader(i)->GetPartitionVersionHashId() !=
                     snapshotPartReader->GetPartitionVersionHashId())) {
            tables.push_back(snapshotPartReader->GetSchema()->GetSchemaName());
        }
        ++readerId;
        if (readerId < mReaderTypeVec.size() && mReaderTypeVec[readerId] == READER_PRIMARY_SUB_TYPE) {
            // ignore sub reader, only check main reader
            // because sub reader has same PartitionVersion with main reader
            ++readerId;
            continue;
        }
    }
}

bool IndexApplication::IsLatestPartitionReader(const IndexPartitionReaderPtr& partitionReader) const
{
    assert(partitionReader);
    const string& tableName = partitionReader->GetSchema()->GetSchemaName();
    auto iter = mTableName2PartitionIdMap.find(tableName);
    if (iter == mTableName2PartitionIdMap.end()) {
        return false;
    }
    uint32_t id = iter->second;
    assert(id < mReaderContainer.Size());
    return mReaderContainer.GetReader(id)->GetPartitionVersionHashId() == partitionReader->GetPartitionVersionHashId();
}

bool IndexApplication::UpdateSnapshotReader(const PartitionReaderSnapshotPtr& snapshotReader,
                                            const string& tableName) const
{
    auto iter = mTableName2PartitionIdMap.find(tableName);
    if (iter == mTableName2PartitionIdMap.end()) {
        return false;
    }
    uint32_t id = iter->second;
    assert(id < mReaderContainer.Size());
    auto latestReader = mReaderContainer.GetReader(id);
    if (latestReader->GetPartitionVersionHashId() !=
        snapshotReader->GetIndexPartitionReader(id)->GetPartitionVersionHashId()) {
        IndexPartitionReaderInfo partitionInfo;
        partitionInfo.indexPartReader = latestReader;
        partitionInfo.tableName = latestReader->GetSchema()->GetSchemaName();
        partitionInfo.partitionIdentifier = mIndexPartitionVec[id]->GetPartitionIdentifier();
        partitionInfo.readerType = mReaderTypeVec[id];
        snapshotReader->UpdateIndexPartitionReaderInfo(id, partitionInfo);
        return true;
    }
    return false;
}

PartitionReaderSnapshotPtr IndexApplication::CreateSnapshot() { return CreateSnapshot(string()); }

PartitionReaderSnapshotPtr IndexApplication::CreateSnapshot(const std::string& leadingTableName, int64_t maxCacheTimeUs)
{
    if (maxCacheTimeUs <= 0) {
        return DoCreateSnapshot(leadingTableName);
    } else {
        bool needInit = false;
        PartitionReaderSnapshotPtr snapshotReader = nullptr;
        {
            autil::ScopedReadLock rlock(mSnapshotLock);
            if (mSnapshotCacheTimeUs != maxCacheTimeUs) {
                needInit = true;
            }
            snapshotReader = mLastPartitionReaderSnapshot;
        }
        if (!snapshotReader) {
            snapshotReader = DoCreateSnapshot(string());
            snapshotReader->InitTableMainSubIdxMap();
            auto curTime = autil::TimeUtility::currentTime();
            {
                autil::ScopedWriteLock wrlock(mSnapshotLock);
                mSnapshotCacheTimeUs = maxCacheTimeUs;
                mLastPartitionReaderSnapshot = snapshotReader;
                mLastSnapshotCreateTime = curTime;
            }
        } else if (needInit) {
            autil::ScopedWriteLock wrlock(mSnapshotLock);
            mSnapshotCacheTimeUs = maxCacheTimeUs;
        }
        return snapshotReader;
    }
}

PartitionReaderSnapshotPtr IndexApplication::DoCreateSnapshot(const std::string& leadingTableName)
{
    vector<IndexPartitionReaderInfo> indexPartReaderInfos;
    GetIndexPartitionReaderInfos(&indexPartReaderInfos);

    if (_hasTablet) {
        std::vector<TabletReaderInfo> tabletReaderInfos;
        GetTabletReaderInfos(&tabletReaderInfos);
        return std::make_shared<PartitionReaderSnapshot>(&mIndexName2IdMap, &mAttrName2IdMap, &mPackAttrName2IdMap,
                                                         indexPartReaderInfos, mReaderUpdater->GetJoinRelationMap(),
                                                         tabletReaderInfos, leadingTableName);
    } else {
        return std::make_shared<PartitionReaderSnapshot>(&mIndexName2IdMap, &mAttrName2IdMap, &mPackAttrName2IdMap,
                                                         indexPartReaderInfos, mReaderUpdater->GetJoinRelationMap(),
                                                         leadingTableName);
    }
}

void IndexApplication::GetTabletReaderInfos(vector<TabletReaderInfo>* tabletReaderInfos)
{
    const std::string TABLE_TYPE_ORC = "orc";
    const std::string TABLE_TYPE_NORMAL = "normal";
    uint32_t baseIdx = _tabletReaderTypeBaseIdx;
    uint32_t readerId = 0;
    for (const auto& tablet : _tablets) {
        auto tabletReader = tablet->GetTabletReader();
        assert(tabletReader != nullptr);
        std::string tableName = tablet->GetTabletSchema()->GetTableName();
        TabletReaderInfo readerInfo;
        readerInfo.tabletReader = tabletReader;
        readerInfo.readerType = mReaderTypeVec[readerId + baseIdx];
        readerInfo.tableName = tableName;
        readerInfo.isLeader = tablet->GetTabletOptions()->IsLeader();
        readerInfo.tabletInfos = tablet->GetTabletInfos();
        auto tabletName = tablet->GetTabletInfos()->GetTabletName();
        readerInfo.partitionIdentifier = autil::HashAlgorithm::hashString64(tabletName.c_str(), tabletName.length());
        tabletReaderInfos->emplace_back(std::move(readerInfo));
        ++readerId;
        if (tablet->GetTabletSchema()->GetTableType() != TABLE_TYPE_NORMAL &&
            tablet->GetTabletSchema()->GetTableType() != TABLE_TYPE_ORC) {
            // TODO: need support other table type ?
            IE_LOG(DEBUG, "un-support table type: [%s]", tablet->GetTabletSchema()->GetTableType().c_str());
            continue;
        }
    }
    assert(readerId + baseIdx == mReaderTypeVec.size());
    return;
}

std::vector<std::shared_ptr<indexlibv2::framework::ITablet>> IndexApplication::GetAllTablets() const
{
    return _tablets;
}

void IndexApplication::GetIndexPartitionReaderInfos(vector<IndexPartitionReaderInfo>* indexPartReaderInfos)
{
    uint32_t readerId = 0;
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    assert(snapshotIndexPartitionReaders.size() == mIndexPartitionVec.size());
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); ++i) {
        const IndexPartitionReaderPtr& mainReader = snapshotIndexPartitionReaders[i];
        assert(mainReader);
        string tableName = mainReader->GetSchema()->GetSchemaName();
        IndexPartitionReaderInfo readerInfo;
        readerInfo.indexPartReader = mainReader;
        readerInfo.readerType = mReaderTypeVec[readerId++];
        readerInfo.tableName = tableName;
        readerInfo.partitionIdentifier = mIndexPartitionVec[i]->GetPartitionIdentifier();
        indexPartReaderInfos->emplace_back(std::move(readerInfo));
        if (mainReader->GetSchema()->GetTableType() == TableType::tt_customized) {
            continue;
        }
        const IndexPartitionReaderPtr& subReader = mainReader->GetSubPartitionReader();
        if (subReader) {
            IndexPartitionReaderInfo readerInfo;
            readerInfo.indexPartReader = subReader;
            readerInfo.readerType = mReaderTypeVec[readerId++];
            readerInfo.tableName = tableName; // sub schema has same name with main schema
            readerInfo.partitionIdentifier = mIndexPartitionVec[i]->GetPartitionIdentifier();
            indexPartReaderInfos->emplace_back(std::move(readerInfo));
        }
    }
    return;
}

void IndexApplication::GetTableSchemas(std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>>& tableSchemas)
{
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++) {
        tableSchemas.push_back(snapshotIndexPartitionReaders[i]->GetTabletSchema());
    }
    for (const auto& tablet : _tablets) {
        tableSchemas.push_back(tablet->GetTabletSchema());
    }
}

std::shared_ptr<indexlibv2::config::ITabletSchema> IndexApplication::GetTableSchema(const string& tableName) const
{
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++) {
        const auto& schema = snapshotIndexPartitionReaders[i]->GetTabletSchema();
        const string& schemaName = schema->GetTableName();
        if (schemaName == tableName) {
            return schema;
        }
    }
    if (_hasTablet) {
        for (const auto& tablet : _tablets) {
            if (tableName == tablet->GetTabletSchema()->GetTableName()) {
                return tablet->GetTabletSchema();
            }
        }
    }
    return nullptr;
}

void IndexApplication::GetTableLatestDataTimestamps(vector<IndexDataTimestampInfo>& dataTsInfos)
{
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++) {
        dataTsInfos.push_back(make_pair(snapshotIndexPartitionReaders[i]->GetSchema()->GetSchemaName(),
                                        snapshotIndexPartitionReaders[i]->GetLatestDataTimestamp()));
    }
}

void IndexApplication::BackgroundUpdateExpiredSnapshotReader()
{
    int64_t curTime = autil::TimeUtility::currentTime();
    bool isExpired = false;
    {
        autil::ScopedReadLock rlock(mSnapshotLock);
        if (mSnapshotCacheTimeUs > 0 && curTime > mSnapshotCacheTimeUs + mLastSnapshotCreateTime) {
            isExpired = true;
        }
    }
    if (isExpired) {
        PartitionReaderSnapshotPtr snapshotReader = DoCreateSnapshot(string());
        snapshotReader->InitTableMainSubIdxMap();
        curTime = autil::TimeUtility::currentTime();
        {
            autil::ScopedWriteLock wrlock(mSnapshotLock);
            if (mSnapshotCacheTimeUs > 0) {
                mLastPartitionReaderSnapshot = snapshotReader;
                mLastSnapshotCreateTime = curTime;
            }
        }
    }
}

void IndexApplication::updateCallBack() { Close(); }

std::vector<std::string> IndexApplication::GetAllSchemaName() const
{
    std::vector<std::string> result;
    vector<IndexPartitionReaderPtr> snapshotIndexPartitionReaders;
    mReaderContainer.CreateSnapshot(snapshotIndexPartitionReaders);
    for (size_t i = 0; i < snapshotIndexPartitionReaders.size(); i++) {
        const auto& schema = snapshotIndexPartitionReaders[i]->GetTabletSchema();
        result.emplace_back(schema->GetTableName());
    }
    if (_hasTablet) {
        for (const auto& tablet : _tablets) {
            result.emplace_back(tablet->GetTabletSchema()->GetTableName());
        }
    }
    return result;
}

bool IndexApplication::IsTablet(const std::string& tableName) const
{
    if (!_hasTablet) {
        return false;
    }
    auto iter = mTableName2PartitionIdMap.find(tableName);
    if (iter == mTableName2PartitionIdMap.end()) {
        return false;
    }
    uint32_t id = iter->second;
    return id >= mIndexPartitionVec.size();
}

}} // namespace indexlib::partition
