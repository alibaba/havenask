#ifndef ISEARCH_FAKEINDEXPARTITIONREADERWRAPPER_H
#define ISEARCH_FAKEINDEXPARTITIONREADERWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>

BEGIN_HA3_NAMESPACE(search);

class FakeIndexPartitionReaderWrapper : public IndexPartitionReaderWrapper
{
public:
    FakeIndexPartitionReaderWrapper(std::map<std::string, uint32_t>* indexName2IdMap, 
                                    std::map<std::string, uint32_t>* attrName2IdMap, 
                                    const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs);
    ~FakeIndexPartitionReaderWrapper();
private:
    FakeIndexPartitionReaderWrapper(const FakeIndexPartitionReaderWrapper &);
    FakeIndexPartitionReaderWrapper& operator=(const FakeIndexPartitionReaderWrapper &);
private:
    std::map<std::string, uint32_t> *_indexName2IdMap;
    std::map<std::string, uint32_t> *_attrName2IdMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeIndexPartitionReaderWrapper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEINDEXPARTITIONREADERWRAPPER_H
