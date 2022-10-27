#ifndef ISEARCH_SORTERMODULEFACTORY_H
#define ISEARCH_SORTERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ModuleFactory.h>
#include <ha3/sorter/Sorter.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/config/TableInfo.h>
#include <suez/turing/expression/plugin/SorterModuleFactory.h>

BEGIN_HA3_NAMESPACE(sorter);

typedef suez::turing::SorterModuleFactory SorterModuleFactory; 

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_SORTERMODULEFACTORY_H
