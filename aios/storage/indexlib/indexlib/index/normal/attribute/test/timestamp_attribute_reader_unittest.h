#ifndef __INDEXLIB_TIMESTAMPATTRIBUTEREADERTEST_H
#define __INDEXLIB_TIMESTAMPATTRIBUTEREADERTEST_H

#include "autil/mem_pool/Pool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/timestamp_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class TimestampAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    TimestampAttributeReaderTest();
    ~TimestampAttributeReaderTest();

    DECLARE_CLASS_NAME(TimestampAttributeReaderTest);
    typedef SingleValueAttributeReader<uint64_t> TimestampAttributeReader;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRead();

private:
    void FullBuild(const std::vector<uint32_t>& docCounts, std::vector<std::string>& ans);

    void CreateDataForOneSegment(segmentid_t segId, uint32_t docCount, std::vector<std::string>& ans);

    void CheckRead(const TimestampAttributeReader& reader, const std::vector<std::string>& ans);

private:
    config::AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool _pool;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimestampAttributeReaderTest, TestRead);
}} // namespace indexlib::index

#endif //__INDEXLIB_TIMESTAMPATTRIBUTEREADERTEST_H
