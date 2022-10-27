#ifndef ISEARCH_BUILDINSORTERMODULEFACTORY_H
#define ISEARCH_BUILDINSORTERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/SorterModuleFactory.h>

BEGIN_HA3_NAMESPACE(sorter);

class BuildinSorterModuleFactory : public SorterModuleFactory
{
public:
    BuildinSorterModuleFactory();
    ~BuildinSorterModuleFactory();
private:
    BuildinSorterModuleFactory(const BuildinSorterModuleFactory &);
    BuildinSorterModuleFactory& operator=(const BuildinSorterModuleFactory &);
public:
    virtual void destroy() { delete this; }
    virtual Sorter* createSorter(const char *sorterName,
                                 const KeyValueMap &sorterParameters,
                                 suez::ResourceReader *resourceReader);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BuildinSorterModuleFactory);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_BUILDINSORTERMODULEFACTORY_H
