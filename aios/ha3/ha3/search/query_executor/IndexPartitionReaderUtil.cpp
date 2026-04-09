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
#include "ha3/search/IndexPartitionReaderUtil.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartialIndexPartitionReaderWrapper.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/build_config_base.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, IndexPartitionReaderUtil);

IndexPartitionReaderUtil::IndexPartitionReaderUtil() {}

IndexPartitionReaderUtil::~IndexPartitionReaderUtil() {}

IndexPartitionReaderWrapperPtr IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
    PartitionReaderSnapshot *partitionReaderSnapshot,
    const string &mainTableName,
    bool usePartial) {
    auto tabletReader = partitionReaderSnapshot->GetTabletReader(mainTableName);
    if (tabletReader) {
        AUTIL_LOG(INFO,
                  "create index partition reader wrapper with tabletReader,"
                  " mainTableName[%s], usePartial[%d]",
                  mainTableName.c_str(),
                  usePartial);
        if (usePartial) {
            return make_shared<PartialIndexPartitionReaderWrapper>(tabletReader);
        } else {
            return make_shared<IndexPartitionReaderWrapper>(tabletReader);
        }
    } else {
        AUTIL_LOG(INFO, "createIndexPartitionReaderWrapper for table 1 %s", mainTableName.c_str());
        const auto &tableMainSubIdxMap = partitionReaderSnapshot->getTableMainSubIdxMap();
        auto iter = tableMainSubIdxMap.find(mainTableName);
        if (iter == tableMainSubIdxMap.end()) {
            IndexPartitionReaderPtr mainIndexPartReader
                = partitionReaderSnapshot->GetIndexPartitionReader(mainTableName);
            if (mainIndexPartReader == NULL) {
                AUTIL_LOG(INFO, "createIndexPartitionReaderWrapper for table 2 %s", mainTableName.c_str());
                return IndexPartitionReaderWrapperPtr();
            }
            AUTIL_LOG(INFO, "createIndexPartitionReaderWrapper for table 3 %s", mainTableName.c_str());
            return createIndexPartitionReaderWrapper(mainIndexPartReader, usePartial);
        } else {
            AUTIL_LOG(INFO, "createIndexPartitionReaderWrapper for table 4 %s", mainTableName.c_str());
            return createIndexPartitionReaderWrapper(&(iter->second.indexName2IdMap),
                                                     &(iter->second.attrName2IdMap),
                                                     &(iter->second.indexReaderVec),
                                                     usePartial,
                                                     false);
        }
    }
}

search::IndexPartitionReaderWrapperPtr IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
    const IndexPartitionReaderPtr &indexPartReader, bool usePartial) {
    map<string, uint32_t> *indexName2IdMap = new map<string, uint32_t>();
    map<string, uint32_t> *attrName2IdMap = new map<string, uint32_t>();
    vector<IndexPartitionReaderPtr> *indexReaderVec = new vector<IndexPartitionReaderPtr>();
    IndexPartitionSchemaPtr schemaPtr = indexPartReader->GetSchema();
    addIndexPartition(
        schemaPtr, IndexPartitionReader::MAIN_PART_ID, *indexName2IdMap, *attrName2IdMap);
    // add sub index partition
    const IndexPartitionSchemaPtr &subPartSchemaPtr = schemaPtr->GetSubIndexPartitionSchema();
    if (subPartSchemaPtr) {
        addIndexPartition(
            subPartSchemaPtr, IndexPartitionReader::SUB_PART_ID, *indexName2IdMap, *attrName2IdMap);
    }
    size_t size = IndexPartitionReader::SUB_PART_ID + 1;
    indexReaderVec->resize(size);
    (*indexReaderVec)[IndexPartitionReader::MAIN_PART_ID] = indexPartReader;
    IndexPartitionReaderPtr subIndexPartReader = indexPartReader->GetSubPartitionReader();
    if (subIndexPartReader) {
        (*indexReaderVec)[IndexPartitionReader::SUB_PART_ID] = subIndexPartReader;
    }
    return createIndexPartitionReaderWrapper(
        indexName2IdMap, attrName2IdMap, indexReaderVec, usePartial, true);
}

search::IndexPartitionReaderWrapperPtr IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
    const map<string, uint32_t> *indexName2IdMap,
    const map<string, uint32_t> *attrName2IdMap,
    const vector<IndexPartitionReaderPtr> *indexReaderVec,
    bool usePartial,
    bool ownMap) {
    IndexPartitionReaderWrapperPtr readerWrapper;
    if (usePartial) {
        readerWrapper.reset(new PartialIndexPartitionReaderWrapper(
            indexName2IdMap, attrName2IdMap, indexReaderVec, ownMap));
    } else {
        readerWrapper.reset(new IndexPartitionReaderWrapper(
            indexName2IdMap, attrName2IdMap, indexReaderVec, ownMap));
    }
    return readerWrapper;
}

void IndexPartitionReaderUtil::addIndexPartition(const IndexPartitionSchemaPtr &schemaPtr,
                                                 uint32_t id,
                                                 map<string, uint32_t> &indexName2IdMap,
                                                 map<string, uint32_t> &attrName2IdMap) {
    const AttributeSchemaPtr &attributeSchemaPtr = schemaPtr->GetAttributeSchema();
    if (attributeSchemaPtr) {
        AttributeSchema::Iterator iter = attributeSchemaPtr->Begin();
        for (; iter != attributeSchemaPtr->End(); iter++) {
            const AttributeConfigPtr &attrConfigPtr = *iter;
            const string &attrName = attrConfigPtr->GetAttrName();
            if (attrName2IdMap.find(attrName) == attrName2IdMap.end()) {
                attrName2IdMap[attrName] = id;
            }
        }
    }
    const IndexSchemaPtr &indexSchemaPtr = schemaPtr->GetIndexSchema();
    if (indexSchemaPtr) {
        IndexSchema::Iterator indexIt = indexSchemaPtr->Begin();
        for (; indexIt != indexSchemaPtr->End(); indexIt++) {
            const IndexConfigPtr &indexConfig = *indexIt;
            const string &indexName = indexConfig->GetIndexName();
            if (indexName2IdMap.find(indexName) == indexName2IdMap.end()) {
                indexName2IdMap[indexName] = id;
            }
        }
    }
}

} // namespace search
} // namespace isearch
