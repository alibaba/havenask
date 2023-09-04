#ifndef __INDEXLIB_TIMEATTRIBUTEREADERTEST_H
#define __INDEXLIB_TIMEATTRIBUTEREADERTEST_H

#include "autil/mem_pool/Pool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/time_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class TimeAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    TimeAttributeReaderTest();
    ~TimeAttributeReaderTest();

    DECLARE_CLASS_NAME(TimeAttributeReaderTest);
    typedef SingleValueAttributeReader<uint32_t> TimeAttributeReader;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRead();

private:
    void FullBuild(const std::vector<uint32_t>& docCounts, std::vector<std::string>& ans);

    void CreateDataForOneSegment(segmentid_t segId, uint32_t docCount, std::vector<std::string>& ans);

    void CheckRead(const TimeAttributeReader& reader, const std::vector<std::string>& ans);

private:
    config::AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool _pool;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeAttributeReaderTest, TestRead);
}} // namespace indexlib::index

#endif //__INDEXLIB_TIMEATTRIBUTEREADERTEST_H
