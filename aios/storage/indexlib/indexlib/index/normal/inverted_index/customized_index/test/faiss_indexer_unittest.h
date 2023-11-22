#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/faiss_indexer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class FaissIndexerTest : public INDEXLIB_TESTBASE
{
public:
    FaissIndexerTest();
    ~FaissIndexerTest();

    DECLARE_CLASS_NAME(FaissIndexerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FaissIndexerTest, TestSimpleProcess);
}} // namespace indexlib::index
