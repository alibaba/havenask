#ifndef ISEARCH_BS_SORTDOCUMENTSORTER_H
#define ISEARCH_BS_SORTDOCUMENTSORTER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/SortDocumentConverter.h"

namespace build_service {
namespace builder {

class SortDocumentSorter
{
public:
    SortDocumentSorter()
        : _pool(new autil::mem_pool::Pool)
    {}
    virtual ~SortDocumentSorter() {}
    
private:
    SortDocumentSorter(const SortDocumentSorter &);
    SortDocumentSorter& operator=(const SortDocumentSorter &);
    
public:
    virtual void sort(std::vector<uint32_t>& sortDocVec) = 0;
    size_t getMemUse() const { return _pool->getUsedBytes(); }
protected:
    std::tr1::shared_ptr<autil::mem_pool::Pool> _pool;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SortDocumentSorter);

}
}

#endif //ISEARCH_BS_SORTDOCUMENTSORTER_H
