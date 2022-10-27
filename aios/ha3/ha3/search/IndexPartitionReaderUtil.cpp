#include <ha3/search/IndexPartitionReaderUtil.h>

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, IndexPartitionReaderUtil);

IndexPartitionReaderUtil::IndexPartitionReaderUtil() {
}

IndexPartitionReaderUtil::~IndexPartitionReaderUtil() {
}

IndexPartitionReaderWrapperPtr IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        PartitionReaderSnapshot *partitionReaderSnapshot, const string& mainTableName)
{
    IndexPartitionReaderPtr mainIndexPartReader =
        partitionReaderSnapshot->GetIndexPartitionReader(mainTableName);
    if (mainIndexPartReader == NULL) {
        return IndexPartitionReaderWrapperPtr();
    }
    std::map<std::string, uint32_t> indexName2IdMap;
    std::map<std::string, uint32_t> attrName2IdMap;
    IndexPartitionSchemaPtr schemaPtr = mainIndexPartReader->GetSchema();
    addIndexPartition(schemaPtr, IndexPartitionReaderWrapper::MAIN_PART_ID,
                      indexName2IdMap, attrName2IdMap);

    // add sub index partition
    const IndexPartitionSchemaPtr &subPartSchemaPtr =  schemaPtr->GetSubIndexPartitionSchema();
    if (subPartSchemaPtr) {
        addIndexPartition(subPartSchemaPtr, IndexPartitionReaderWrapper::SUB_PART_ID,
                          indexName2IdMap, attrName2IdMap);
    }

    std::vector<IndexPartitionReaderPtr> indexPartReaderPtrs;
    size_t size = IndexPartitionReaderWrapper::JOIN_PART_START_ID;
    indexPartReaderPtrs.resize(size);

    indexPartReaderPtrs[0] = mainIndexPartReader;
    IndexPartitionReaderPtr subIndexPartReader =  mainIndexPartReader->GetSubPartitionReader();
    if (subIndexPartReader) {
        indexPartReaderPtrs[1] = subIndexPartReader;
    }
    IndexPartitionReaderWrapperPtr readerWrapper;
    readerWrapper.reset(new IndexPartitionReaderWrapper(indexName2IdMap,
                    attrName2IdMap, indexPartReaderPtrs));
    return readerWrapper;
}

void IndexPartitionReaderUtil::addIndexPartition(const IndexPartitionSchemaPtr &schemaPtr,
        uint32_t id, std::map<std::string, uint32_t> &indexName2IdMap,
        std::map<std::string, uint32_t> &attrName2IdMap)
{
    const AttributeSchemaPtr &attributeSchemaPtr = schemaPtr->GetAttributeSchema();
    if (attributeSchemaPtr) {
        AttributeSchema::Iterator iter = attributeSchemaPtr->Begin();
        for(; iter != attributeSchemaPtr->End(); iter++) {
            const AttributeConfigPtr &attrConfigPtr = *iter;
            const string& attrName = attrConfigPtr->GetAttrName();
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


END_HA3_NAMESPACE(search);
