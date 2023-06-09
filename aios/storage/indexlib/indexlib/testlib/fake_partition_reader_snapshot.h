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
#ifndef __INDEXLIB_FAKE_PARTITION_READER_SNAPSHOT_H
#define __INDEXLIB_FAKE_PARTITION_READER_SNAPSHOT_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_reader_snapshot.h"

namespace indexlib { namespace testlib {

class FakePartitionReaderSnapshot : public partition::PartitionReaderSnapshot
{
public:
    FakePartitionReaderSnapshot(partition::TableMem2IdMap* indexName2IdMap, partition::TableMem2IdMap* attrName2IdMap,
                                partition::TableMem2IdMap* packAttrName2IdMap,
                                const std::vector<partition::IndexPartitionReaderInfo>& indexPartReaderInfos)
        : PartitionReaderSnapshot(indexName2IdMap, attrName2IdMap, packAttrName2IdMap, indexPartReaderInfos)
        , mIndexName2IdMap(indexName2IdMap)
        , mAttrName2IdMap(attrName2IdMap)
        , mPackAttrName2IdMap(packAttrName2IdMap)
    {
    }

    FakePartitionReaderSnapshot(partition::TableMem2IdMap* indexName2IdMap, partition::TableMem2IdMap* attrName2IdMap,
                                partition::TableMem2IdMap* packAttrName2IdMap,
                                const std::vector<partition::IndexPartitionReaderInfo>& indexPartReaderInfos,
                                const std::vector<partition::TabletReaderInfo>& tabletReaderInfos)
        : PartitionReaderSnapshot(indexName2IdMap, attrName2IdMap, packAttrName2IdMap, indexPartReaderInfos, nullptr,
                                  tabletReaderInfos, "")
        , mIndexName2IdMap(indexName2IdMap)
        , mAttrName2IdMap(attrName2IdMap)
        , mPackAttrName2IdMap(packAttrName2IdMap)
    {
    }

    ~FakePartitionReaderSnapshot()
    {
        DELETE_AND_SET_NULL(mIndexName2IdMap);
        DELETE_AND_SET_NULL(mAttrName2IdMap);
        DELETE_AND_SET_NULL(mPackAttrName2IdMap);
    }

private:
    partition::TableMem2IdMap* mIndexName2IdMap;
    partition::TableMem2IdMap* mAttrName2IdMap;
    partition::TableMem2IdMap* mPackAttrName2IdMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePartitionReaderSnapshot);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_PARTITION_READER_SNAPSHOT_H
