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
#ifndef __INDEXLIB_FAKEPARTITIONREADERSNAPSHOTCREATOR_H
#define __INDEXLIB_FAKEPARTITIONREADERSNAPSHOTCREATOR_H

#include <memory>

#include "future_lite/coro/Lazy.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_reader_snapshot.h"

namespace indexlibv2 {
namespace config {
class TabletSchema;
}
namespace framework {
class ITabletReader;
class Tablet;
} // namespace framework
} // namespace indexlibv2
namespace future_lite {
class Executor;
class TaskScheduler;
} // namespace future_lite

namespace indexlib { namespace testlib {

struct FakeIndex {
    FakeIndex()
    {
        versionId = 0;
        stringValueSep = ",";
        fieldSep = ":";
        tableName = "table";
    }
    ~FakeIndex() {}
    std::string rootPath;

    std::map<std::string, std::string> indexes;

    // summary format:
    //    field1,field2,field3,...,fieldN;   ---> doc1
    //    field1,field2,field3,...,fieldN;   ---> doc2
    std::string summarys;
    // attribute format:
    //    attrName:attrType:doc1,doc2,...,docN;
    //    attrName:attrType:doc1,doc2,...,docN;
    std::string attributes;
    // const attribute format:
    //    attrName:attrType;
    //    attrName:attrType;
    std::string constAttributes;
    // deletion map format:
    //    doc1,doc2,...,docN
    std::string deletedDocIds;
    std::string partitionMeta;
    // segmentDocCount1,segmentDocCount2|rtSegmentDocCount
    std::string segmentDocCounts;
    std::string stringValueSep;
    std::string fieldSep;
    versionid_t versionId;
    uint32_t totalDocCount;
    std::string tableName;
    std::vector<std::string> tablePrefix;
    indexlib::config::IndexPartitionSchemaPtr tableSchema;
    bool useTablet = false;
    std::string tabletSchemaStr;
    std::string indexSep = ","; // attribute/inverted_index/summary

    void clear()
    {
        indexes.clear();
        summarys.clear();
        attributes.clear();
        deletedDocIds.clear();
        partitionMeta.clear();
        versionId = 0;
        tablePrefix.clear();
        useTablet = false;
    }
};

class FakeSummaryReader;
class FakeIndexPartitionReader;
class FakePartitionReaderSnapshotCreator
{
public:
    FakePartitionReaderSnapshotCreator();
    ~FakePartitionReaderSnapshotCreator();

public:
    static partition::PartitionReaderSnapshotPtr createIndexPartitionReader(const FakeIndex& fakeIndex);
    static partition::PartitionReaderSnapshotPtr createIndexPartitionReader(const std::vector<FakeIndex>& fakeIndexs);

private:
    static void innerCreateIndexPartitionReader(const FakeIndex& fakeIndex, partition::TableMem2IdMap* indexName2IdMap,
                                                partition::TableMem2IdMap* attrName2IdMap,
                                                partition::TableMem2IdMap* packAttrName2IdMap,
                                                std::vector<FakeIndexPartitionReader*>& indexPartReaders,
                                                std::vector<partition::IndexPartitionReaderInfo>& indexPartReaderInfos);
    static void innerCreateTabletReader(const FakeIndex& fakeIndex, partition::TableMem2IdMap* indexName2IdMap,
                                        partition::TableMem2IdMap* attrName2IdMap,
                                        partition::TableMem2IdMap* packAttrName2IdMap,
                                        std::vector<partition::TabletReaderInfo>& tabletReaderInfos,
                                        size_t partitionReaderSize);

    static FakeSummaryReader* createFakeSummaryReader(const std::string& summaryStr);

    static void initIndexPartReaders(const FakeIndex& fakeIndex, const std::map<std::string, uint32_t>& prefixToIdx,
                                     const std::vector<FakeIndexPartitionReader*>& indexPartVec,
                                     partition::TableMem2IdMap& indexName2IdMap,
                                     partition::TableMem2IdMap& attrName2IdMap, size_t idxBegin);
    static void fillIndexInfoMap(const FakeIndex& fakeIndex, partition::TableMem2IdMap& indexName2IdMap,
                                 partition::TableMem2IdMap& attrName2IdMap, size_t idxBegin);

    static uint32_t getIndexPartReaderIdx(const std::map<std::string, uint32_t>& prefixToIdx,
                                          const std::string& orgIdentifier, std::string& newIdentifier,
                                          uint32_t defaultIdx);
    static void createFakeAttributeReader(FakeIndexPartitionReader* fakePartitionReader, const std::string& attrName,
                                          const std::string& type, const std::string& values,
                                          const std::string& rootPath, const std::string& stringValueSep);
    static void initSegmentDocCounts(FakeIndexPartitionReader* reader, std::string segDocCountsStr);
    static index::DeletionMapReaderPtr createDeletionMapReader(const std::string& deletedDocIdStr,
                                                               uint32_t totalDocCount);
    static std::shared_ptr<indexlibv2::config::TabletSchema> GetDefaultTabletSchema(const FakeIndex& fakeIndex);
    static std::shared_ptr<indexlibv2::framework::Tablet>
    GetDefaultTablet(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema, const FakeIndex& fakeIndex);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePartitionReaderSnapshotCreator);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKEPARTITIONREADERSNAPSHOTCREATOR_H
