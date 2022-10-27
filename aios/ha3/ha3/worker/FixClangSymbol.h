#ifndef ISEARCH_FIXCLANGSYMBOL_H
#define ISEARCH_FIXCLANGSYMBOL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>

BEGIN_HA3_NAMESPACE(worker);

class FixClangSymbol
{
public:
    FixClangSymbol();
    ~FixClangSymbol();
private:
    FixClangSymbol(const FixClangSymbol &);
    FixClangSymbol& operator=(const FixClangSymbol &);
public:
    IE_NAMESPACE(index)::IndexReduceItem *createIndexReduceItem();
    void notUse();
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FixClangSymbol);

END_HA3_NAMESPACE(worker);

#endif //ISEARCH_FIXCLANGSYMBOL_H
