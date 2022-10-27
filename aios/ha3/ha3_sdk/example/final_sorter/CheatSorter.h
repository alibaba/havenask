#ifndef ISEARCH_CHEATSORTER_H
#define ISEARCH_CHEATSORTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/Sorter.h>
BEGIN_HA3_NAMESPACE(sorter);

class CheatSorter : public Sorter
{
public:
    CheatSorter(const std::string &name);
    CheatSorter(const CheatSorter &other);
    ~CheatSorter();
public:
    /* override */ Sorter* clone();
    /* override */ bool beginSort(suez::turing::SorterProvider *provider);
    /* override */ void sort(SortParam &sortParam);
    /* override */ void endSort();
    /* override */ void destroy();
private:
    void fillCheatIDs(const std::string &cheatIdStr);
protected:
    rank::ComboComparator *_comp;
    std::set<int32_t> _cheatIDSet;
    const matchdoc::Reference<int32_t> *_idAttributRef; 
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CheatSorter);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_CHEATSORTER_H
