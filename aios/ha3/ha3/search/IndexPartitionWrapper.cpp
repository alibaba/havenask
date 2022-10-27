#include <ha3/search/IndexPartitionWrapper.h>
#include <autil/StringUtil.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/config/ClusterTableInfoManager.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/virtual_attribute_config_creator.h>
#include <indexlib/partition/join_cache/join_cache_initializer_creator.h>
#include <indexlib/partition/table_reader_container_updater.h>
#include <ha3/search/PartialIndexPartitionReaderWrapper.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

USE_HA3_NAMESPACE(config);
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, IndexPartitionWrapper);

IndexPartitionWrapper::IndexPartitionWrapper() {
}

IndexPartitionWrapper::~IndexPartitionWrapper() {
}

bool IndexPartitionWrapper::init(
        const IndexPartitionPtr &indexPart)
{
    addMainIndexPartition(indexPart);
    std::unordered_map<std::string, uint32_t> tableName2PartitionIdMap;
    vector<IndexPartitionPtr> indexPartitions;
    string mainTableName = _mainIndexPart->GetSchema()->GetSchemaName();
    tableName2PartitionIdMap[mainTableName] = 0;
    indexPartitions.push_back(_mainIndexPart);

    return true;
}


void IndexPartitionWrapper::addMainIndexPartition(
        const IndexPartitionPtr &indexPart)
{
    // add main index partition
    _mainIndexPart = indexPart;
    IndexPartitionSchemaPtr schemaPtr = indexPart->GetSchema();
    assert(schemaPtr);
    addIndexPartition(schemaPtr, IndexPartitionReaderWrapper::MAIN_PART_ID);
    // add sub index partition
    const IndexPartitionSchemaPtr &subPartSchemaPtr =
        schemaPtr->GetSubIndexPartitionSchema();
    if (subPartSchemaPtr) {
        addIndexPartition(subPartSchemaPtr,
                          IndexPartitionReaderWrapper::SUB_PART_ID);
    }
}

bool IndexPartitionWrapper::isSubField(const IndexPartitionSchemaPtr &schemaPtr,
                                       const string &joinField) const
{
    const IndexPartitionSchemaPtr &subSchema = schemaPtr->GetSubIndexPartitionSchema();
    if (!subSchema) {
        return false;
    }
    const AttributeSchemaPtr &subAttrSchema = subSchema->GetAttributeSchema();
    return subAttrSchema && subAttrSchema->IsInAttribute(joinField);
}

void IndexPartitionWrapper::addIndexPartition(
        const IndexPartitionSchemaPtr &schemaPtr, uint32_t id)
{
    const AttributeSchemaPtr &attributeSchemaPtr = schemaPtr->GetAttributeSchema();
    if (attributeSchemaPtr) {
        AttributeSchema::Iterator iter = attributeSchemaPtr->Begin();
        for(; iter != attributeSchemaPtr->End(); iter++) {
            const AttributeConfigPtr &attrConfigPtr = *iter;
            const string& attrName = attrConfigPtr->GetAttrName();
            if (_attrName2IdMap.find(attrName) == _attrName2IdMap.end()) {
                _attrName2IdMap[attrName] = id;
            }
        }
    }
    const IndexSchemaPtr &indexSchemaPtr = schemaPtr->GetIndexSchema();
    if (indexSchemaPtr) {
        IndexSchema::Iterator indexIt = indexSchemaPtr->Begin();
        for (; indexIt != indexSchemaPtr->End(); indexIt++) {
            const IndexConfigPtr &indexConfig = *indexIt;
            const string &indexName = indexConfig->GetIndexName();
            if (_indexName2IdMap.find(indexName) == _indexName2IdMap.end()) {
                _indexName2IdMap[indexName] = id;
            }
        }
    }
}

bool IndexPartitionWrapper::getIndexPartReaders(
        IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr indexSnapshot,
        vector<IndexPartitionReaderPtr> &indexPartReaderPtrs) const
{
    size_t size = IndexPartitionReaderWrapper::JOIN_PART_START_ID;
    indexPartReaderPtrs.resize(size);
    string mainTableName = getMainTableName();
    IndexPartitionReaderPtr mainIndexPartReader =
        indexSnapshot->GetIndexPartitionReader(mainTableName);
    if (!mainIndexPartReader) {
        return false;
    }
    indexPartReaderPtrs[0] = mainIndexPartReader;
    IndexPartitionReaderPtr subIndexPartReader =
        mainIndexPartReader->GetSubPartitionReader();
    if (subIndexPartReader) {
        indexPartReaderPtrs[1] = subIndexPartReader;
    }
    return true;
}

IndexPartitionReaderWrapperPtr IndexPartitionWrapper::createReaderWrapper(
        IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &indexSnapshot) const
{
    vector<IndexPartitionReaderPtr> indexPartReaderPtrs;
    if (!getIndexPartReaders(indexSnapshot, indexPartReaderPtrs)) {
        return IndexPartitionReaderWrapperPtr();
    }
    IndexPartitionReaderWrapperPtr readerWrapper;
    readerWrapper.reset(new IndexPartitionReaderWrapper(&_indexName2IdMap,
                    &_attrName2IdMap, indexPartReaderPtrs));
    return readerWrapper;
}

IndexPartitionReaderWrapperPtr IndexPartitionWrapper::createPartialReaderWrapper(
        IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &indexSnapshot) const
{
    vector<IndexPartitionReaderPtr> indexPartReaderPtrs;
    if (!getIndexPartReaders(indexSnapshot, indexPartReaderPtrs)) {
        return IndexPartitionReaderWrapperPtr();
    }
    IndexPartitionReaderWrapperPtr readerWrapper;
    readerWrapper.reset(new PartialIndexPartitionReaderWrapper(&_indexName2IdMap,
                    &_attrName2IdMap, indexPartReaderPtrs));
    return readerWrapper;
}

versionid_t IndexPartitionWrapper::getCurrentVersion() const {
    IndexPartitionReaderPtr reader = _mainIndexPart->GetReader();
    const Version &version = reader->GetVersion();
    return version.GetVersionId();
}

IndexPartitionWrapperPtr IndexPartitionWrapper::createIndexPartitionWrapper(
        const ConfigAdapterPtr &configAdapterPtr,
        const IndexPartitionPtr &indexPart,
        const string &clusterName)
{
    assert(configAdapterPtr);
    ClusterConfigMapPtr clusterConfigMapPtr = configAdapterPtr->getClusterConfigMap();
    if (!clusterConfigMapPtr) {
        HA3_LOG(ERROR, "get cluster config map failed!");
        return IndexPartitionWrapperPtr();
    }
    ClusterConfigMap::const_iterator iter =
        clusterConfigMapPtr->find(clusterName);
    if (iter == clusterConfigMapPtr->end()) {
        HA3_LOG(ERROR, "get cluster config for cluster [%s] failed!",
                clusterName.c_str());
        return IndexPartitionWrapperPtr();
    }
    IndexPartitionWrapperPtr indexPartitionWrapperPtr(new IndexPartitionWrapper);
    if (!indexPartitionWrapperPtr->init(indexPart))
    {
        HA3_LOG(ERROR, "init IndexPartitionWrapper failed");
        indexPartitionWrapperPtr.reset();
    }
    return indexPartitionWrapperPtr;
}

END_HA3_NAMESPACE(search);

