#ifndef ISEARCH_SORTERMODULEFACTORYFORTEST_H
#define ISEARCH_SORTERMODULEFACTORYFORTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/SorterModuleFactory.h>

BEGIN_HA3_NAMESPACE(sorter);

class FakeSorter : public suez::turing::Sorter {
public:
    FakeSorter(const std::string &sorterName)
        : Sorter(sorterName)
    {
    }
    virtual ~FakeSorter() {}
public:
    /* override */ bool beginSort(suez::turing::SorterProvider *provider) {
        return true;
    }
    /* override */ suez::turing::Sorter* clone() {
        return new FakeSorter(*this);
    }

    /* override */ void sort(suez::turing::SortParam &sortParam) {
    }

    /* override */ void endSort() {
    }

    /* override */ void destroy() {
        delete this;
    }
public:
    std::string getTableName() const {
        return "";
    }

};


class SorterModuleFactoryForTest: public suez::turing::SorterModuleFactory
{
public:
    SorterModuleFactoryForTest();
    ~SorterModuleFactoryForTest();
public:
    /* override */ bool init(const KeyValueMap &parameters) {
        return true;
    }
    /* override */ 
    suez::turing::Sorter* createSorter(const char *sorterName, 
                         const KeyValueMap &parameters,
                         suez::ResourceReader *resourceReader);
    /* override */
    void destroy() {
        delete this;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SorterModuleFactoryForTest);
HA3_TYPEDEF_PTR(FakeSorter);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_SORTERMODULEFACTORYFORTEST_H
