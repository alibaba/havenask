#ifndef __INDEXLIB_OFFLINEPARTITIONREADERTEST_H
#define __INDEXLIB_OFFLINEPARTITIONREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/offline_partition_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
namespace indexlib { namespace partition {

class OfflinePartitionReaderTest : public INDEXLIB_TESTBASE
{
public:
    OfflinePartitionReaderTest();
    ~OfflinePartitionReaderTest();

    DECLARE_CLASS_NAME(OfflinePartitionReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCacheLoadConfig();
    void TestSkipLoadIndex();
    void TestUseLazyLoadAttributeReaders();

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OfflinePartitionReaderTest, TestCacheLoadConfig);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionReaderTest, TestSkipLoadIndex);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionReaderTest, TestUseLazyLoadAttributeReaders);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINEPARTITIONREADERTEST_H
