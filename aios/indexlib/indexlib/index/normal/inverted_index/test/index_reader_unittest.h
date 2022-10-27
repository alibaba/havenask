#ifndef __INDEXLIB_INDEXREADERTEST_H
#define __INDEXLIB_INDEXREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/partition/online_partition.h"

IE_NAMESPACE_BEGIN(index);

class IndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    IndexReaderTest();
    ~IndexReaderTest();

    DECLARE_CLASS_NAME(IndexReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexReaderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEXREADERTEST_H
