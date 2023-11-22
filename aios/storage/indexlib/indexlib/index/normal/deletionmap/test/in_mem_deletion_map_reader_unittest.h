#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class InMemDeletionMapReaderTest : public INDEXLIB_TESTBASE
{
public:
    InMemDeletionMapReaderTest();
    ~InMemDeletionMapReaderTest();

    DECLARE_CLASS_NAME(InMemDeletionMapReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemDeletionMapReaderTest, TestSimpleProcess);
}} // namespace indexlib::index
