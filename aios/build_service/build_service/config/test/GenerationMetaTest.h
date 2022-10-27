#ifndef ISEARCH_GENERATIONMETATEST_H
#define ISEARCH_GENERATIONMETATEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/GenerationMeta.h>
#include <cppunit/extensions/HelperMacros.h>

BEGIN_HA3_NAMESPACE(config);

class GenerationMetaTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(GenerationMetaTest);
    CPPUNIT_TEST(testSimpleProcess);
    CPPUNIT_TEST_SUITE_END();
public:
    GenerationMetaTest();
    ~GenerationMetaTest();
public:
    void setUp();
    void tearDown();
    void testSimpleProcess();
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(config);

#endif //ISEARCH_GENERATIONMETATEST_H
