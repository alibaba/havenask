#ifndef ISEARCH_SORTER_H
#define ISEARCH_SORTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/SorterProvider.h>
#include <suez/turing/expression/plugin/Sorter.h>

BEGIN_HA3_NAMESPACE(sorter);

typedef suez::turing::SortParam SortParam; 
typedef suez::turing::Sorter Sorter; 

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_SORTER_H
