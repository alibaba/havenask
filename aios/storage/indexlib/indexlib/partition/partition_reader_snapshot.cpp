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
#include "indexlib/partition/partition_reader_snapshot.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionReaderSnapshot);

PartitionReaderSnapshot::PartitionReaderSnapshot()
    : mIndex2IdMap(nullptr)
    , mAttribute2IdMap(nullptr)
    , mPackAttribute2IdMap(nullptr)
    , mReverseJoinRelations(nullptr)
{
}

PartitionReaderSnapshot::PartitionReaderSnapshot(const TableMem2IdMap* indexName2IdMap,
                                                 const TableMem2IdMap* attrName2IdMap,
                                                 const TableMem2IdMap* packAttrName2IdMap,
                                                 const vector<IndexPartitionReaderInfo>& indexPartReaderInfos,
                                                 const ReverseJoinRelationMap* reverseJoinRelations,
                                                 const string& leadingTableName)
    : mIndex2IdMap(indexName2IdMap)
    , mAttribute2IdMap(attrName2IdMap)
    , mPackAttribute2IdMap(packAttrName2IdMap)
    , mIndexPartReaderInfos(indexPartReaderInfos)
    , mReverseJoinRelations(reverseJoinRelations)
    , mLeadingTable(leadingTableName)
{
    assert(mIndex2IdMap && mAttribute2IdMap && packAttrName2IdMap);
    for (uint32_t i = 0; i < mIndexPartReaderInfos.size(); i++) {
        if (indexPartReaderInfos[i].readerType != READER_PRIMARY_SUB_TYPE) {
            const string& tableName = mIndexPartReaderInfos[i].tableName;
            assert(mTableName2Idx.find(tableName) == mTableName2Idx.end());
            mTableName2Idx[tableName] = i;
            // mPartitions[tableName] = indexPartReaderInfos[i].indexPartReader.get();
        }
    }

    InitName2IdxMap(indexName2IdMap, mIndexName2Idx);
    InitName2IdxMap(attrName2IdMap, mAttributeName2Idx);
    InitName2IdxMap(packAttrName2IdMap, mPackAttributeName2Idx);
}

PartitionReaderSnapshot::PartitionReaderSnapshot(const TableMem2IdMap* indexName2IdMap,
                                                 const TableMem2IdMap* attrName2IdMap,
                                                 const TableMem2IdMap* packAttrName2IdMap,
                                                 const std::vector<IndexPartitionReaderInfo>& indexPartReaderInfos,
                                                 const ReverseJoinRelationMap* reverseJoinRelations,
                                                 const std::vector<TabletReaderInfo>& tabletReaderInfos,
                                                 const string& leadingTableName)
    : mIndex2IdMap(indexName2IdMap)
    , mAttribute2IdMap(attrName2IdMap)
    , mPackAttribute2IdMap(packAttrName2IdMap)
    , mIndexPartReaderInfos(indexPartReaderInfos)
    , _tabletReaderInfos(tabletReaderInfos)
    , mReverseJoinRelations(reverseJoinRelations)
    , mLeadingTable(leadingTableName)
{
    assert(mIndex2IdMap && mAttribute2IdMap && packAttrName2IdMap);
    for (uint32_t i = 0; i < mIndexPartReaderInfos.size(); ++i) {
        if (mIndexPartReaderInfos[i].readerType != READER_PRIMARY_SUB_TYPE) {
            const string& tableName = mIndexPartReaderInfos[i].tableName;
            assert(mTableName2Idx.find(tableName) == mTableName2Idx.end());
            mTableName2Idx[tableName] = i;
        }
    }
    uint32_t baseIdx = mIndexPartReaderInfos.size();
    for (uint32_t i = 0; i < _tabletReaderInfos.size(); ++i) {
        if (_tabletReaderInfos[i].readerType != READER_PRIMARY_SUB_TYPE) {
            const string& tableName = _tabletReaderInfos[i].tableName;
            assert(mTableName2Idx.find(tableName) == mTableName2Idx.end());
            mTableName2Idx[tableName] = i + baseIdx;
        }
        mTabletLeaderInfos[_tabletReaderInfos[i].tableName] = _tabletReaderInfos[i].isLeader;
        if (_tabletReaderInfos[i].tabletInfos != nullptr) {
            mTabletLocatorInfos[_tabletReaderInfos[i].tableName] = _tabletReaderInfos[i].tabletInfos;
        }
    }

    InitName2IdxMap(indexName2IdMap, mIndexName2Idx);
    InitName2IdxMap(attrName2IdMap, mAttributeName2Idx);
    InitName2IdxMap(packAttrName2IdMap, mPackAttributeName2Idx);
}

PartitionReaderSnapshot::~PartitionReaderSnapshot() {}

void PartitionReaderSnapshot::InitName2IdxMap(const TableMem2IdMap* member2IdMap, Name2IdMap& name2IdMap)
{
    vector<string> toEraseName;
    TableMem2IdMap::Iterator iter = member2IdMap->Begin();
    for (; iter != member2IdMap->End(); iter++) {
        const string& tableName = (iter->first).first;
        if (!IsLeadingRelatedTable(tableName)) {
            continue;
        }

        const string& name = (iter->first).second;
        if (name2IdMap.find(name) == name2IdMap.end()) {
            name2IdMap[name] = iter->second;
        } else {
            toEraseName.push_back(name);
        }
    }
    // duplicate name erase
    for (size_t i = 0; i < toEraseName.size(); i++) {
        name2IdMap.erase(toEraseName[i]);
    }
}

void PartitionReaderSnapshot::InitTableMainSubIdxMap()
{
    for (uint32_t i = 0; i < mIndexPartReaderInfos.size(); i++) {
        if (mIndexPartReaderInfos[i].readerType != READER_PRIMARY_SUB_TYPE) {
            TableMainSubIdx& mainSubIdx = mTableMainSubIdxMap[mIndexPartReaderInfos[i].tableName];
            const config::IndexPartitionSchemaPtr& schemaPtr = mIndexPartReaderInfos[i].indexPartReader->GetSchema();
            AddMainSubIdxFromSchema(schemaPtr, IndexPartitionReader::MAIN_PART_ID, mainSubIdx.indexName2IdMap,
                                    mainSubIdx.attrName2IdMap);
            // add sub index partition
            const config::IndexPartitionSchemaPtr& subPartSchemaPtr = schemaPtr->GetSubIndexPartitionSchema();
            if (subPartSchemaPtr) {
                AddMainSubIdxFromSchema(subPartSchemaPtr, IndexPartitionReader::SUB_PART_ID, mainSubIdx.indexName2IdMap,
                                        mainSubIdx.attrName2IdMap);
            }
            size_t size = IndexPartitionReader::SUB_PART_ID + 1;
            mainSubIdx.indexReaderVec.resize(size);
            mainSubIdx.indexReaderVec[IndexPartitionReader::MAIN_PART_ID] = mIndexPartReaderInfos[i].indexPartReader;
            IndexPartitionReaderPtr subIndexPartReader =
                mIndexPartReaderInfos[i].indexPartReader->GetSubPartitionReader();
            if (subIndexPartReader) {
                mainSubIdx.indexReaderVec[IndexPartitionReader::SUB_PART_ID] = subIndexPartReader;
            }
        }
    }
}

bool PartitionReaderSnapshot::GetIdxByName(const string& name, const Name2IdMap& map, uint32_t& idx) const
{
    Name2IdMap::const_iterator iter = map.find(name);
    if (iter == map.end()) {
        return false;
    }
    idx = iter->second;
    return true;
}

std::shared_ptr<InvertedIndexReader> PartitionReaderSnapshot::GetInvertedIndexReader(const string& tableName,
                                                                                     const string& indexName) const
{
    uint32_t readerId = 0;
    if (tableName.empty()) {
        if (!GetIdxByName(indexName, mIndexName2Idx, readerId)) {
            IE_LOG(ERROR, "can not find index reader from table [%s] for index[%s]", tableName.c_str(),
                   indexName.c_str());
            return std::shared_ptr<InvertedIndexReader>();
        }
    } else {
        if (!mIndex2IdMap->Find(tableName, indexName, readerId)) {
            IE_LOG(ERROR, "can not find index reader from table [%s] for index[%s]", tableName.c_str(),
                   indexName.c_str());
            return std::shared_ptr<InvertedIndexReader>();
        }
    }
    if (readerId >= mIndexPartReaderInfos.size()) {
        IE_LOG(ERROR, "Invalid readIdx, table [%s], index[%s], idx[%u], size[%lu]", tableName.c_str(),
               indexName.c_str(), readerId, mIndexPartReaderInfos.size());
        return std::shared_ptr<InvertedIndexReader>();
    }
    return mIndexPartReaderInfos[readerId].indexPartReader->GetInvertedIndexReader();
}

const IndexPartitionReaderInfo* PartitionReaderSnapshot::GetIndexPartitionReaderInfo(const string& attrName,
                                                                                     const string& tableName) const
{
    uint32_t readerId = 0;
    if (GetReaderIdByAttrNameAndTableName(attrName, tableName, readerId)) {
        if (readerId >= mIndexPartReaderInfos.size()) {
            return nullptr;
        }
        return &mIndexPartReaderInfos[readerId];
    }
    return nullptr;
}

bool PartitionReaderSnapshot::GetReaderIdByAttrNameAndTableName(const string& attrName, const string& tableName,
                                                                uint32_t& readerId) const
{
    if (tableName.empty()) {
        if (!GetIdxByName(attrName, mAttributeName2Idx, readerId)) {
            IE_LOG(ERROR, "can not find attribute reader [%s] in table [%s]", attrName.c_str(), tableName.c_str());
            return false;
        }
    } else if (!attrName.empty()) {
        if (!mAttribute2IdMap->Find(tableName, attrName, readerId)) {
            IE_LOG(ERROR, "can not find attribute reader [%s] in table [%s]", attrName.c_str(), tableName.c_str());
            return false;
        }
    } else {
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        if (iter == mTableName2Idx.end()) {
            return false;
        }
        readerId = iter->second;
    }
    return true;
}

bool PartitionReaderSnapshot::GetIndexPartitionReaderInfo(const string& attrName, const string& tableName,
                                                          IndexPartitionReaderInfo& indexPartReaderInfo) const
{
    uint32_t readerId = 0;
    if (!GetReaderIdByAttrNameAndTableName(attrName, tableName, readerId)) {
        return false;
    }
    if (readerId >= mIndexPartReaderInfos.size()) {
        return false;
    }
    indexPartReaderInfo = mIndexPartReaderInfos[readerId];
    return true;
}

bool PartitionReaderSnapshot::GetTabletReaderInfo(const string& attrName, const string& tableName,
                                                  TabletReaderInfo& tabletReaderInfo) const
{
    uint32_t readerId = 0;
    if (!GetReaderIdByAttrNameAndTableName(attrName, tableName, readerId)) {
        return false;
    }
    if (readerId < mIndexPartReaderInfos.size()) {
        return false;
    }
    readerId = readerId - mIndexPartReaderInfos.size();
    assert(readerId < _tabletReaderInfos.size());
    tabletReaderInfo = _tabletReaderInfos[readerId];
    return true;
}

bool PartitionReaderSnapshot::GetAttributeReaderInfo(const string& attrName, const string& tableName,
                                                     RawPointerAttributeReaderInfo& attrReaderInfo) const
{
    const IndexPartitionReaderInfo* readerInfo = GetIndexPartitionReaderInfo(attrName, tableName);
    if (!readerInfo) {
        return false;
    }

    const AttributeReaderPtr& attrReader = readerInfo->indexPartReader->GetAttributeReader(attrName);
    if (!attrReader) {
        IE_LOG(ERROR, "attribute reader [%s] in table [%s] is null", attrName.c_str(), tableName.c_str());
        return false;
    }
    attrReaderInfo.attrReader = attrReader.get();
    attrReaderInfo.indexPartReader = readerInfo->indexPartReader.get();
    attrReaderInfo.indexPartReaderType = readerInfo->readerType;
    return true;
}

bool PartitionReaderSnapshot::GetAttributeReaderInfo(const string& attrName, const string& tableName,
                                                     AttributeReaderInfo& attrReaderInfo) const
{
    IndexPartitionReaderInfo readerInfo;
    if (!GetIndexPartitionReaderInfo(attrName, tableName, readerInfo)) {
        return false;
    }
    attrReaderInfo.attrReader = readerInfo.indexPartReader->GetAttributeReader(attrName);
    if (!attrReaderInfo.attrReader) {
        IE_LOG(ERROR, "attribute reader [%s] in table [%s] is null", attrName.c_str(), tableName.c_str());
        return false;
    }
    attrReaderInfo.indexPartReader = readerInfo.indexPartReader;
    attrReaderInfo.indexPartReaderType = readerInfo.readerType;
    return true;
}

bool PartitionReaderSnapshot::GetAttributeReaderInfo(const string& attrName, const string& tableName,
                                                     AttributeReaderInfoV2& attrReaderInfo) const
{
    TabletReaderInfo readerInfo;
    if (!GetTabletReaderInfo(attrName, tableName, readerInfo)) {
        return false;
    }
    auto normalTabletReader =
        std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(readerInfo.tabletReader);
    assert(normalTabletReader != nullptr);
    attrReaderInfo.attrReader = normalTabletReader->GetAttributeReader(attrName);
    if (!attrReaderInfo.attrReader) {
        IE_LOG(ERROR, "attribute reader [%s] in table [%s] is null", attrName.c_str(), tableName.c_str());
        return false;
    }
    attrReaderInfo.tabletReader = readerInfo.tabletReader;
    attrReaderInfo.indexPartReaderType = readerInfo.readerType;
    return true;
}

bool PartitionReaderSnapshot::GetPackAttributeReaderInfo(const string& packAttrName, const string& tableName,
                                                         PackAttributeReaderInfo& attrReaderInfo) const
{
    uint32_t readerId = 0;
    if (tableName.empty()) {
        if (!GetIdxByName(packAttrName, mPackAttributeName2Idx, readerId)) {
            IE_LOG(ERROR, "can not find pack attribute reader [%s] in table [%s]", packAttrName.c_str(),
                   tableName.c_str());
            return false;
        }
    } else {
        if (!mPackAttribute2IdMap->Find(tableName, packAttrName, readerId)) {
            IE_LOG(ERROR, "can not find pack attribute reader [%s] in table [%s]", packAttrName.c_str(),
                   tableName.c_str());
            return false;
        }
    }
    if (readerId >= mIndexPartReaderInfos.size()) {
        IE_LOG(ERROR, "invalid idx, packAttr [%s], table [%s], idx[%u], size[%lu]", packAttrName.c_str(),
               tableName.c_str(), readerId, mIndexPartReaderInfos.size());
        return false;
    }
    IndexPartitionReaderInfo readerInfo = mIndexPartReaderInfos[readerId];
    attrReaderInfo.packAttrReader = readerInfo.indexPartReader->GetPackAttributeReader(packAttrName);
    attrReaderInfo.indexPartReader = readerInfo.indexPartReader;
    attrReaderInfo.indexPartReaderType = readerInfo.readerType;
    return true;
}

std::string PartitionReaderSnapshot::GetTableNameByAttribute(const std::string& attrName,
                                                             const std::string& preferTableName)
{
    uint32_t readerId = 0;
    // find in preferTableName first, since conflict field not in mAttributeName2Idx
    if (mAttribute2IdMap->Exist(preferTableName, attrName)) {
        return preferTableName;
    }
    // find all table
    if (!GetIdxByName(attrName, mAttributeName2Idx, readerId)) {
        return "";
    } else {
        if (readerId < mIndexPartReaderInfos.size()) { // for index partition
            auto indexPartReader = mIndexPartReaderInfos[readerId].indexPartReader;
            return indexPartReader->GetSchema()->GetSchemaName();
        }
        // for tablet
        readerId = readerId - mIndexPartReaderInfos.size();
        assert(readerId < _tabletReaderInfos.size());
        auto tabletReader = _tabletReaderInfos[readerId].tabletReader;
        auto normalReader = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(tabletReader);
        assert(normalReader != nullptr);
        return normalReader->GetSchema()->GetTableName();
    }
}

std::string PartitionReaderSnapshot::GetTableNameByPackAttribute(const std::string& packAttrName,
                                                                 const std::string& preferTableName)
{
    uint32_t readerId = 0;
    // find in preferTableName first, since conflict field not in mAttributeName2Idx
    if (mPackAttribute2IdMap->Exist(preferTableName, packAttrName)) {
        return preferTableName;
    }
    // find all table
    if (!GetIdxByName(packAttrName, mPackAttributeName2Idx, readerId)) {
        return "";
    } else {
        assert(readerId < mIndexPartReaderInfos.size());
        auto indexPartReader = mIndexPartReaderInfos[readerId].indexPartReader;
        return indexPartReader->GetSchema()->GetSchemaName();
    }
}

bool PartitionReaderSnapshot::tableName2SchemaMap(
    std::unordered_map<std::string, std::pair<bool, IndexPartitionSchemaPtr>>& schemaMaps)
{
    for (auto readerInfo : mIndexPartReaderInfos) {
        if (readerInfo.readerType == READER_PRIMARY_SUB_TYPE) {
            continue;
        }
        auto reader = readerInfo.indexPartReader;
        if (!reader) {
            IE_LOG(ERROR, "table[%s]'s reader is null", readerInfo.tableName.c_str());
            return false;
        }
        schemaMaps[readerInfo.tableName] = make_pair(false, reader->GetSchema());
    }
    for (auto readerInfo : _tabletReaderInfos) {
        if (readerInfo.readerType == READER_PRIMARY_SUB_TYPE) {
            continue;
        }
        auto reader = readerInfo.tabletReader;
        if (!reader) {
            IE_LOG(ERROR, "tablet[%s]'s reader is null", readerInfo.tableName.c_str());
            return false;
        }
        auto kvTabletSessionReader = dynamic_cast<indexlibv2::table::KVTabletSessionReader*>(reader.get());
        if (!kvTabletSessionReader) {
            IE_LOG(WARN, "tablet[%s]'s reader can not trans to common tablet reader, maybe user define tablet",
                   readerInfo.tableName.c_str());
            continue;
        }
        schemaMaps[readerInfo.tableName] = make_pair(true, kvTabletSessionReader->GetSchema()->GetLegacySchema());
    }
    return true;
}

bool PartitionReaderSnapshot::InitAttributeJoinInfo(const string& mainTableName, const string& joinTableName,
                                                    const string& joinKey, const JoinField& joinField,
                                                    JoinInfo& joinInfo) const
{
    AttributeReaderInfo attrReaderInfo;
    if (!GetAttributeReaderInfo(joinKey, mainTableName, attrReaderInfo)) {
        IE_LOG(ERROR, "join key: [%s] not exist in table [%s]", joinKey.c_str(), mainTableName.c_str());
        return false;
    }

    IndexPartitionReaderType mainPartitionReaderType = attrReaderInfo.indexPartReaderType;
    if (mainPartitionReaderType != READER_PRIMARY_MAIN_TYPE && mainPartitionReaderType != READER_PRIMARY_SUB_TYPE) {
        IE_LOG(ERROR, "Table[%s] whose type is [%u] is not supported for join", mainTableName.c_str(),
               mainPartitionReaderType);
        return false;
    }
    const IndexPartitionReaderInfo& joinReaderInfo = GetIndexPartitionReaderInfo(joinTableName);
    joinInfo.Init(mainPartitionReaderType == READER_PRIMARY_SUB_TYPE, joinField, joinReaderInfo.partitionIdentifier,
                  attrReaderInfo.indexPartReader, joinReaderInfo.indexPartReader);
    return true;
}

bool PartitionReaderSnapshot::GetAttributeJoinInfo(const string& joinTableName, const string& mainTableName,
                                                   JoinInfo& joinInfo) const
{
    if (!mReverseJoinRelations) {
        IE_LOG(ERROR, "Reverse join relation table is not set.");
        return false;
    }
    ReverseJoinRelationMap::const_iterator it = mReverseJoinRelations->find(joinTableName);
    if (it == mReverseJoinRelations->end()) {
        IE_LOG(ERROR, "Cannot find join table[%s] in join relation map", joinTableName.c_str());
        return false;
    }
    // key: mainTableName
    const JoinFieldMap& joinInfoMap = it->second;
    JoinFieldMap::const_iterator joinIter;
    // mainTableName can be omitted iff JoinTable is used by one MainTable
    if (mainTableName.empty()) {
        joinIter = (joinInfoMap.size() == 1) ? joinInfoMap.begin() : joinInfoMap.end();
    } else {
        joinIter = joinInfoMap.find(mainTableName);
    }

    if (joinIter == joinInfoMap.end()) {
        IE_LOG(ERROR, "Cannot find main table for join table[%s]", joinTableName.c_str());
        return false;
    }

    const string& mainTable = joinIter->first;
    const JoinField& joinField = joinIter->second;
    const string& joinKey = joinField.fieldName;
    return InitAttributeJoinInfo(mainTable, joinTableName, joinKey, joinField, joinInfo);
}

bool PartitionReaderSnapshot::IsLeadingRelatedTable(const string& tableName) const
{
    if (mLeadingTable.empty() || tableName == mLeadingTable) {
        return true;
    }
    return IsJoinTable(tableName, mLeadingTable);
}

bool PartitionReaderSnapshot::IsJoinTable(const string& joinTableName, const string& mainTableName) const
{
    if (!mReverseJoinRelations) {
        return false;
    }
    ReverseJoinRelationMap::const_iterator it = mReverseJoinRelations->find(joinTableName);
    if (it == mReverseJoinRelations->end()) {
        return false;
    }
    // key: mainTableName
    typedef map<string, JoinField> JoinInfoMap;
    const JoinInfoMap& joinInfoMap = it->second;
    JoinInfoMap::const_iterator joinIter = joinInfoMap.find(mainTableName);
    return joinIter != joinInfoMap.end();
}

PartitionReaderSnapshot* PartitionReaderSnapshot::CreateLeadingSnapshotReader(const string& leadingTableName)
{
    std::unique_ptr<PartitionReaderSnapshot> readerSnapshot(
        new PartitionReaderSnapshot(mIndex2IdMap, mAttribute2IdMap, mPackAttribute2IdMap, mIndexPartReaderInfos,
                                    mReverseJoinRelations, leadingTableName));
    return readerSnapshot.release();
}

void PartitionReaderSnapshot::AddMainSubIdxFromSchema(const config::IndexPartitionSchemaPtr& schemaPtr, uint32_t id,
                                                      std::map<std::string, uint32_t>& indexName2IdMap,
                                                      std::map<std::string, uint32_t>& attrName2IdMap)
{
    const config::AttributeSchemaPtr& attributeSchemaPtr = schemaPtr->GetAttributeSchema();
    if (attributeSchemaPtr) {
        config::AttributeSchema::Iterator iter = attributeSchemaPtr->Begin();
        for (; iter != attributeSchemaPtr->End(); iter++) {
            const config::AttributeConfigPtr& attrConfigPtr = *iter;
            const string& attrName = attrConfigPtr->GetAttrName();
            if (attrName2IdMap.find(attrName) == attrName2IdMap.end()) {
                attrName2IdMap[attrName] = id;
            }
        }
    }
    const config::IndexSchemaPtr& indexSchemaPtr = schemaPtr->GetIndexSchema();
    if (indexSchemaPtr) {
        config::IndexSchema::Iterator indexIt = indexSchemaPtr->Begin();
        for (; indexIt != indexSchemaPtr->End(); indexIt++) {
            const config::IndexConfigPtr& indexConfig = *indexIt;
            const string& indexName = indexConfig->GetIndexName();
            if (indexName2IdMap.find(indexName) == indexName2IdMap.end()) {
                indexName2IdMap[indexName] = id;
            }
        }
    }
}

bool PartitionReaderSnapshot::isLeader(const std::string& tableName) const
{
    auto iter = mTabletLeaderInfos.find(tableName);
    if (iter == mTabletLeaderInfos.end()) {
        return false;
    }
    return iter->second;
}

int64_t PartitionReaderSnapshot::getBuildOffset(const std::string& tableName) const
{
    auto iter = mTabletLocatorInfos.find(tableName);
    if (iter == mTabletLocatorInfos.end()) {
        return -1;
    }
    auto locator = iter->second->GetBuildLocator();
    return locator.GetOffset();
}

}} // namespace indexlib::partition
