#ifndef ISEARCH_NULLSORTER_H
#define ISEARCH_NULLSORTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/Sorter.h>

BEGIN_HA3_NAMESPACE(sorter);

class NullSorter : public suez::turing::Sorter
{
public:
    NullSorter();
    ~NullSorter();
private:
    NullSorter& operator=(const NullSorter &);
public:
    Sorter* clone() override;

    bool beginSort(suez::turing::SorterProvider *provider) override;

    void sort(suez::turing::SortParam &sortParam) override;

    void endSort() override;

    void destroy() override {
        delete this;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(NullSorter);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_NULLSORTER_H
