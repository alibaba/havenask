#ifndef ISEARCH_OPTIMIZERCHAINMANAGERCREATORTEST_H
#define ISEARCH_OPTIMIZERCHAINMANAGERCREATORTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OptimizerChainManagerCreator.h>
#include <unittest/unittest.h>

BEGIN_HA3_NAMESPACE(search);

class OptimizerChainManagerCreatorTest : public TESTBASE {
public:
    OptimizerChainManagerCreatorTest();
    ~OptimizerChainManagerCreatorTest();
public:
    void setUp();
    void tearDown();
    void testSimpleProcess();
public:
    static OptimizerChainManagerPtr createOptimizerChainManager(const std::string &configStr);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_OPTIMIZERCHAINMANAGERCREATORTEST_H
