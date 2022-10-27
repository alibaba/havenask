#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionReaderSnapshot);

PartitionReaderSnapshot::PartitionReaderSnapshot(
            const TableMem2IdMap *indexName2IdMap,
            const TableMem2IdMap *attrName2IdMap,
            const TableMem2IdMap *packAttrName2IdMap,
            const vector<IndexPartitionReaderInfo> &indexPartReaderInfos,
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
    for (uint32_t i = 0; i < indexPartReaderInfos.size(); i++)
    {
        if (indexPartReaderInfos[i].readerType != READER_PRIMARY_SUB_TYPE)
        {
            const string &tableName = indexPartReaderInfos[i].tableName;
            assert(mTableName2Idx.find(tableName) == mTableName2Idx.end());
            mTableName2Idx[tableName] = i;
            // mPartitions[tableName] = indexPartReaderInfos[i].indexPartReader.get();
        }
    }

    InitName2IdxMap(indexName2IdMap, mIndexName2Idx);
    InitName2IdxMap(attrName2IdMap, mAttributeName2Idx);
    InitName2IdxMap(packAttrName2IdMap, mPackAttributeName2Idx);
}

PartitionReaderSnapshot::~PartitionReaderSnapshot() 
{
}

void PartitionReaderSnapshot::InitName2IdxMap(const TableMem2IdMap* member2IdMap,
                                              Name2IdMap& name2IdMap)
{
    vector<string> toEraseName;
    TableMem2IdMap::const_iterator iter = member2IdMap->begin();
    for (; iter != member2IdMap->end(); iter++)
    {
        const string& tableName = (iter->first).first;
        if (!IsLeadingRelatedTable(tableName))
        {
            continue;
        }
        
        const string& name = (iter->first).second;
        if (name2IdMap.find(name) == name2IdMap.end())
        {
            name2IdMap[name] = iter->second;
        }
        else
        {
            toEraseName.push_back(name);
        }
    }
    //duplicate name erase
    for (size_t i = 0; i < toEraseName.size(); i++)
    {
        name2IdMap.erase(toEraseName[i]);
    }
}

bool PartitionReaderSnapshot::GetIdxByName(
        const string& name, const Name2IdMap& map, uint32_t& idx) const
{
    Name2IdMap::const_iterator iter = map.find(name);
    if (iter == map.end())
    {
        return false;
    }
    idx = iter->second;
    return true;
}

IndexReaderPtr PartitionReaderSnapshot::GetIndexReader(
        const string &tableName, const string &indexName) const
{
    uint32_t readerId = 0;
    if (tableName.empty())
    {
        if (!GetIdxByName(indexName, mIndexName2Idx, readerId))
        {
            IE_LOG(ERROR, "can not find index reader from table [%s] for index[%s]",
                   tableName.c_str(), indexName.c_str());
            return IndexReaderPtr();
        }
    }
    else
    {
        TableMem2IdMap::const_iterator iter = 
            mIndex2IdMap->find(make_pair(tableName, indexName));
        if (iter == mIndex2IdMap->end())
        {
            IE_LOG(ERROR, "can not find index reader from table [%s] for index[%s]",
               tableName.c_str(), indexName.c_str());
            return IndexReaderPtr();
        }
        readerId = iter->second;
    }
    assert(readerId < mIndexPartReaderInfos.size());
    return mIndexPartReaderInfos[readerId].indexPartReader->GetIndexReader();
}

const IndexPartitionReaderInfo* PartitionReaderSnapshot::GetIndexPartitionReaderInfo(
        const string &attrName, const string &tableName) const
{
    uint32_t readerId = 0;
    if (GetReaderIdByAttrNameAndTableName(attrName, tableName, readerId)) {
        assert(readerId < mIndexPartReaderInfos.size());
        return &mIndexPartReaderInfos[readerId];
    }
    return nullptr;
}

bool PartitionReaderSnapshot::GetReaderIdByAttrNameAndTableName(
        const string &attrName, const string &tableName, uint32_t &readerId) const
{
    if (tableName.empty())
    {
        if (!GetIdxByName(attrName, mAttributeName2Idx, readerId))
        {
            IE_LOG(ERROR, "can not find attribute reader [%s] in table [%s]",
                   attrName.c_str(), tableName.c_str());
            return false;
        }
    } else if (!attrName.empty()) {
        TableMem2IdMap::const_iterator iter = mAttribute2IdMap->find(
                make_pair(tableName, attrName));
        if (iter == mAttribute2IdMap->end())
        {
            IE_LOG(ERROR, "can not find attribute reader [%s] in table [%s]",
                   attrName.c_str(), tableName.c_str());
            return false;
        }
        readerId = iter->second;
    } else {
        Name2IdMap::const_iterator iter = mTableName2Idx.find(tableName);
        if (iter == mTableName2Idx.end()) {
            return false;
        }
        readerId = iter->second;
    }
    return true;
}

bool PartitionReaderSnapshot::GetIndexPartitionReaderInfo(
        const string &attrName,
        const string &tableName,
        IndexPartitionReaderInfo &indexPartReaderInfo) const
{
    uint32_t readerId = 0;
    if (!GetReaderIdByAttrNameAndTableName(attrName, tableName, readerId)) {
        return false;
    }
    assert(readerId < mIndexPartReaderInfos.size());
    indexPartReaderInfo = mIndexPartReaderInfos[readerId];
    return true;
}

bool PartitionReaderSnapshot::GetAttributeReaderInfo(
        const string &attrName, const string &tableName,
        RawPointerAttributeReaderInfo &attrReaderInfo) const
{
    const IndexPartitionReaderInfo *readerInfo = GetIndexPartitionReaderInfo(attrName, tableName);
    if (!readerInfo) {
        return false;
    }

    const AttributeReaderPtr& attrReader = 
        readerInfo->indexPartReader->GetAttributeReader(attrName);
    if (!attrReader)
    {
        IE_LOG(ERROR, "attribute reader [%s] in table [%s] is null",
               attrName.c_str(), tableName.c_str());
        return false;
    }
    attrReaderInfo.attrReader = attrReader.get();
    attrReaderInfo.indexPartReader = readerInfo->indexPartReader.get();
    attrReaderInfo.indexPartReaderType = readerInfo->readerType;
    return true;
}

bool PartitionReaderSnapshot::GetAttributeReaderInfo(const string &attrName,
        const string &tableName, AttributeReaderInfo &attrReaderInfo) const
{
    IndexPartitionReaderInfo readerInfo;
    if (!GetIndexPartitionReaderInfo(attrName, tableName, readerInfo))
    {
        return false;
    }
    attrReaderInfo.attrReader = 
        readerInfo.indexPartReader->GetAttributeReader(attrName);
    if (!attrReaderInfo.attrReader)
    {
        IE_LOG(ERROR, "attribute reader [%s] in table [%s] is null",
               attrName.c_str(), tableName.c_str());
        return false;
    }
    attrReaderInfo.indexPartReader = readerInfo.indexPartReader;
    attrReaderInfo.indexPartReaderType = readerInfo.readerType;
    return true;
}

bool PartitionReaderSnapshot::GetPackAttributeReaderInfo(
        const string &packAttrName,
        const string &tableName,
        PackAttributeReaderInfo &attrReaderInfo) const
{
    uint32_t readerId = 0;
    if (tableName.empty())
    {
        if (!GetIdxByName(packAttrName, mPackAttributeName2Idx, readerId))
        {
            IE_LOG(ERROR, "can not find pack attribute reader [%s] in table [%s]",
                   packAttrName.c_str(), tableName.c_str());
            return false;
        }
    }
    else
    {
        TableMem2IdMap::const_iterator iter = 
            mPackAttribute2IdMap->find(make_pair(tableName, packAttrName));
        if (iter == mPackAttribute2IdMap->end())
        {
            IE_LOG(ERROR, "can not find pack attribute reader [%s] in table [%s]",
                   packAttrName.c_str(), tableName.c_str());
            return false;
        }
        readerId = iter->second;
    }
    assert(readerId < mIndexPartReaderInfos.size());
    IndexPartitionReaderInfo readerInfo = mIndexPartReaderInfos[readerId];
    attrReaderInfo.packAttrReader = 
        readerInfo.indexPartReader->GetPackAttributeReader(packAttrName);
    attrReaderInfo.indexPartReader = readerInfo.indexPartReader;
    attrReaderInfo.indexPartReaderType = readerInfo.readerType;
    return true;
}

std::string PartitionReaderSnapshot::GetTableNameByAttribute(const std::string &attrName,
        const std::string &preferTableName)
{
    uint32_t readerId = 0;
    // find in preferTableName first, since conflict field not in mAttributeName2Idx
    TableMem2IdMap::const_iterator iter = mAttribute2IdMap->find(
            make_pair(preferTableName, attrName));
    if (iter != mAttribute2IdMap->end()) {
        return preferTableName;
    }
    // find all table
    if (!GetIdxByName(attrName, mAttributeName2Idx, readerId)) {
        return "";
    } else {
        assert(readerId < mIndexPartReaderInfos.size());
        auto indexPartReader = mIndexPartReaderInfos[readerId].indexPartReader;
        assert(readerId < mIndexPartReaderInfos.size());
        return indexPartReader->GetSchema()->GetSchemaName();
    }
}

std::string PartitionReaderSnapshot::GetTableNameByPackAttribute(const std::string &packAttrName,
        const std::string &preferTableName)
{
    uint32_t readerId = 0;
    // find in preferTableName first, since conflict field not in mAttributeName2Idx
    TableMem2IdMap::const_iterator iter =
        mPackAttribute2IdMap->find(make_pair(preferTableName, packAttrName));
    if (iter != mPackAttribute2IdMap->end()) {
        return preferTableName;
    }
    // find all table
    if (!GetIdxByName(packAttrName, mPackAttributeName2Idx, readerId)) {
        return "";
    } else {
        assert(readerId < mIndexPartReaderInfos.size());
        auto indexPartReader = mIndexPartReaderInfos[readerId].indexPartReader;
        assert(readerId < mIndexPartReaderInfos.size());
        return indexPartReader->GetSchema()->GetSchemaName();
    }
}

bool PartitionReaderSnapshot::InitAttributeJoinInfo(
    const string& mainTableName,
    const string& joinTableName,
    const string& joinKey,
    const JoinField& joinField,
    JoinInfo& joinInfo) const
{
    AttributeReaderInfo attrReaderInfo;
    if (!GetAttributeReaderInfo(joinKey, mainTableName, attrReaderInfo))
    {
        IE_LOG(ERROR, "join key: [%s] not exist in table [%s]",
               joinKey.c_str(), mainTableName.c_str());
        return false;
    }

    IndexPartitionReaderType mainPartitionReaderType =
        attrReaderInfo.indexPartReaderType;
    if (mainPartitionReaderType != READER_PRIMARY_MAIN_TYPE
        && mainPartitionReaderType != READER_PRIMARY_SUB_TYPE)
    {
        IE_LOG(ERROR, "Table[%s] whose type is [%u] is not supported for join",
               mainTableName.c_str(), mainPartitionReaderType);
        return false;
    }
    const IndexPartitionReaderInfo& joinReaderInfo = GetIndexPartitionReaderInfo(joinTableName);
    joinInfo.Init(mainPartitionReaderType == READER_PRIMARY_SUB_TYPE,
                  joinField, joinReaderInfo.partitionIdentifier,
                  attrReaderInfo.indexPartReader,
                  joinReaderInfo.indexPartReader);
    return true;
}

bool PartitionReaderSnapshot::GetAttributeJoinInfo(
    const string& joinTableName,
    const string& mainTableName,
    JoinInfo& joinInfo) const
{
    if (!mReverseJoinRelations)
    {
        IE_LOG(ERROR, "Reverse join relation table is not set.");
        return false;
    }
    ReverseJoinRelationMap::const_iterator it =
        mReverseJoinRelations->find(joinTableName);
    if (it == mReverseJoinRelations->end())
    {
        IE_LOG(ERROR, "Cannot find join table[%s] in join relation map",
               joinTableName.c_str());
        return false;
    }
    // key: mainTableName
    typedef map<string, JoinField> JoinInfoMap;
    const JoinInfoMap& joinInfoMap = it->second;
    JoinInfoMap::const_iterator joinIter;
    // mainTableName can be omitted iff JoinTable is used by one MainTable
    if (mainTableName.empty())
    {
        joinIter = (joinInfoMap.size() == 1) ?
            joinInfoMap.begin() : joinInfoMap.end();
    }
    else
    {
        joinIter = joinInfoMap.find(mainTableName);
    }
    
    if (joinIter == joinInfoMap.end())
    {
        IE_LOG(ERROR, "Cannot find main table for join table[%s]",
               joinTableName.c_str());
        return false;
    }

    const string& mainTable = joinIter->first;
    const JoinField& joinField = joinIter->second;
    const string& joinKey = joinField.fieldName;
    return InitAttributeJoinInfo(
        mainTable, joinTableName, joinKey, joinField, joinInfo);
}

bool PartitionReaderSnapshot::IsLeadingRelatedTable(const string& tableName) const
{
    if (mLeadingTable.empty() || tableName == mLeadingTable)
    {
        return true;
    }
    return IsJoinTable(tableName, mLeadingTable);
}

bool PartitionReaderSnapshot::IsJoinTable(
        const string& joinTableName, const string& mainTableName) const
{
    if (!mReverseJoinRelations)
    {
        return false;
    }
    ReverseJoinRelationMap::const_iterator it =
        mReverseJoinRelations->find(joinTableName);
    if (it == mReverseJoinRelations->end())
    {
        return false;
    }
    // key: mainTableName
    typedef map<string, JoinField> JoinInfoMap;
    const JoinInfoMap& joinInfoMap = it->second;
    JoinInfoMap::const_iterator joinIter = joinInfoMap.find(mainTableName);
    return joinIter != joinInfoMap.end();
}

PartitionReaderSnapshot* PartitionReaderSnapshot::CreateLeadingSnapshotReader(
        const string& leadingTableName)
{
    std::unique_ptr<PartitionReaderSnapshot> readerSnapshot(
            new PartitionReaderSnapshot(mIndex2IdMap, mAttribute2IdMap,
                    mPackAttribute2IdMap, mIndexPartReaderInfos,
                    mReverseJoinRelations, leadingTableName));
    return readerSnapshot.release();
}

IE_NAMESPACE_END(partition);

