#ifndef ISEARCH_FAKEPARTIALINDEXPARTITIONREADERWRAPPER_H
#define ISEARCH_FAKEPARTIALINDEXPARTITIONREADERWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/PartialIndexPartitionReaderWrapper.h>

BEGIN_HA3_NAMESPACE(search);

class FakePartialIndexPartitionReaderWrapper : public PartialIndexPartitionReaderWrapper
{
public:
    FakePartialIndexPartitionReaderWrapper(std::map<std::string, uint32_t>* indexName2IdMap,
                                    std::map<std::string, uint32_t>* attrName2IdMap,
                                    const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs);
    ~FakePartialIndexPartitionReaderWrapper();
private:
    FakePartialIndexPartitionReaderWrapper(const FakePartialIndexPartitionReaderWrapper &);
    FakePartialIndexPartitionReaderWrapper& operator=(const FakePartialIndexPartitionReaderWrapper &);
private:
    std::map<std::string, uint32_t> *_indexName2IdMap;
    std::map<std::string, uint32_t> *_attrName2IdMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakePartialIndexPartitionReaderWrapper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEINDEXPARTITIONREADERWRAPPER_H
