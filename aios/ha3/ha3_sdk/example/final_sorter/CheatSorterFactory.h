#ifndef ISEARCH_CHEATSORTERFACTORY_H
#define ISEARCH_CHEATSORTERFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/SorterModuleFactory.h>
#include <ha3/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(sorter);

class CheatSorterFactory: public SorterModuleFactory
{
public:
    CheatSorterFactory();
    ~CheatSorterFactory();
private:
    CheatSorterFactory(const CheatSorterFactory &);
    CheatSorterFactory& operator=(const CheatSorterFactory &);
public:
    /* override */ bool init(const KeyValueMap&);
    /* override */ Sorter* createSorter(const char *sorterName, 
            const KeyValueMap &parameters,
            config::ResourceReader *resourceReader);
    /* override */ void destroy();
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_PLUGIN40SORTERFACTORY_H
