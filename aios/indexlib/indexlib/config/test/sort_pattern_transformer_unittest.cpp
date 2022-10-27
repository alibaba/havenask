#include "indexlib/config/test/sort_pattern_transformer_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SortPatternTransformerTest);

SortPatternTransformerTest::SortPatternTransformerTest()
{
}

SortPatternTransformerTest::~SortPatternTransformerTest()
{
}

void SortPatternTransformerTest::CaseSetUp()
{
}

void SortPatternTransformerTest::CaseTearDown()
{
}

void SortPatternTransformerTest::TestSimpleProcess()
{
    INDEXLIB_TEST_EQUAL(sp_desc, SortPatternTransformer::FromString(""));
    INDEXLIB_TEST_EQUAL(sp_desc, SortPatternTransformer::FromString("aaa"));

    INDEXLIB_TEST_EQUAL(sp_asc, SortPatternTransformer::FromString("asc"));
    INDEXLIB_TEST_EQUAL(sp_asc, SortPatternTransformer::FromString("ASC"));
    INDEXLIB_TEST_EQUAL(sp_asc, SortPatternTransformer::FromString("Asc"));

    INDEXLIB_TEST_EQUAL(sp_desc, SortPatternTransformer::FromString("desc"));
    INDEXLIB_TEST_EQUAL(sp_desc, SortPatternTransformer::FromString("DESC"));
    INDEXLIB_TEST_EQUAL(sp_desc, SortPatternTransformer::FromString("deSc"));
}

IE_NAMESPACE_END(config);

