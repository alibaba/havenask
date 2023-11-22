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
#include <unordered_map>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/table_reader_container.h"

DECLARE_REFERENCE_CLASS(partition, TableReaderContainerUpdater);
DECLARE_REFERENCE_CLASS(partition, PartitionReaderSnapshot);
DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlibv2::framework {
class ITablet;
}

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlib { namespace partition {

class IndexApplication
{
public:
    // tableName --> IndexPartition
    typedef std::map<std::string, IndexPartitionPtr> IndexPartitionMap;
    // first: tableName, second: data ts
    typedef std::pair<std::string, int64_t> IndexDataTimestampInfo;

    using TabletMap = std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>>;

public:
    IndexApplication();
    ~IndexApplication();

public:
    bool Init(const std::vector<IndexPartitionPtr>& indexPartitions, const JoinRelationMap& joinRelations);

    // TODO: remain for legacy interface caller
    bool Init(const IndexPartitionMap& indexPartitions, const JoinRelationMap& joinRelations);
    bool InitByTablet(const TabletMap& tabletMap);
    // for single offline partition reader
    bool Init(const std::vector<IndexPartitionPtr>& indexPartitions);
    PartitionReaderSnapshotPtr CreateSnapshot();
    PartitionReaderSnapshotPtr CreateSnapshot(const std::string& leadingTableName, int64_t maxCacheTimeUs = 0);
    void GetTableSchemas(std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>>& tableSchemas);
    std::shared_ptr<indexlibv2::config::ITabletSchema> GetTableSchema(const std::string& tableName) const;
    void GetTableLatestDataTimestamps(std::vector<IndexDataTimestampInfo>& dataTsInfos);

    bool IsLatestSnapshotReader(const PartitionReaderSnapshotPtr& snapshotReader) const;

    void GetExpireReaderNames(const PartitionReaderSnapshotPtr& snapshotReader, std::vector<std::string>& tables) const;

    bool UpdateSnapshotReader(const PartitionReaderSnapshotPtr& snapshotReader, const std::string& tableName) const;

    bool IsLatestPartitionReader(const IndexPartitionReaderPtr& partitionReader) const;
    bool IsTablet(const std::string& tableName) const;
    bool HasTablet() const { return _hasTablet; }
    std::vector<std::shared_ptr<indexlibv2::framework::ITablet>> GetAllTablets() const;
    std::vector<std::string> GetAllSchemaName() const;
    void Close();

private:
    PartitionReaderSnapshotPtr DoCreateSnapshot(const std::string& leadingTableName);
    bool AddIndexPartitions(const std::vector<IndexPartitionPtr>& indexPartitions);
    bool AddIndexPartitionsFromVec();
    bool AddIndexPartition(const config::IndexPartitionSchemaPtr& schema, uint32_t id);
    bool AddTablet(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, uint32_t id);
    // bool ParseJoinRelations(const JoinRelationMap &joinRelations);
    // bool ParseJoinRelation(const std::string &mainTableName,
    //                        const std::vector<JoinField> &joinFields);
    void ReviseReaderType(const std::string& tableName, const std::string& joinFieldName);
    void RevisePrimaryReaderId(const std::string& mainTableName, const std::string& joinTableName,
                               const std::string& joinFieldName);

    void updateCallBack();

    bool AddTablets();
    void GetIndexPartitionReaderInfos(std::vector<IndexPartitionReaderInfo>* indexPartReaderInfos);
    void GetTabletReaderInfos(std::vector<TabletReaderInfo>* tabletReaderInfos);
    void BackgroundUpdateExpiredSnapshotReader();
    bool PrepareBackgroundSnapshotReaderThread();

private:
    autil::ReadWriteLock mSnapshotLock;
    int64_t mBackgroundUpdateCacheSnapshotTimeUs = -1;
    int64_t mLastSnapshotCreateTime = -1;
    int64_t mSnapshotCacheTimeUs = -1;
    PartitionReaderSnapshotPtr mLastPartitionReaderSnapshot;
    autil::LoopThreadPtr mUpdateExpiredSnapshotThread;
    std::vector<IndexPartitionPtr> mIndexPartitionVec;
    std::vector<IndexPartitionReaderType> mReaderTypeVec;
    std::unordered_map<std::string, uint32_t> mTableName2PartitionIdMap;
    TableMem2IdMap mIndexName2IdMap;
    TableMem2IdMap mAttrName2IdMap;
    TableMem2IdMap mPackAttrName2IdMap;
    TableReaderContainer mReaderContainer;
    TableReaderContainerUpdaterPtr mReaderUpdater;

    bool _hasTablet = false;
    std::vector<std::shared_ptr<indexlibv2::framework::ITablet>> _tablets;
    size_t _tabletReaderTypeBaseIdx = 0; // offset in mReaderTypeVec

private:
    friend class IndexApplicationTest;
    friend class ReaderSnapshotTest;
    friend class AuxTableInteTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexApplication);
}} // namespace indexlib::partition
