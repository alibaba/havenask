#include <ha3_sdk/testlib/search/FakeIndexPartitionReaderWrapper.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeIndexPartitionReaderWrapper);

FakeIndexPartitionReaderWrapper::FakeIndexPartitionReaderWrapper(
        std::map<std::string, uint32_t>* indexName2IdMap, 
        std::map<std::string, uint32_t>* attrName2IdMap, 
        const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs)
    : IndexPartitionReaderWrapper(indexName2IdMap, attrName2IdMap, indexPartReaderPtrs)
    , _indexName2IdMap(indexName2IdMap)
    , _attrName2IdMap(attrName2IdMap)
{    
}

FakeIndexPartitionReaderWrapper::~FakeIndexPartitionReaderWrapper() { 
    DELETE_AND_SET_NULL(_indexName2IdMap);
    DELETE_AND_SET_NULL(_attrName2IdMap);
}

END_HA3_NAMESPACE(search);

