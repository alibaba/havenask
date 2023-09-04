#ifndef __INDEXLIB_DATEATTRIBUTEREADERTEST_H
#define __INDEXLIB_DATEATTRIBUTEREADERTEST_H

#include "autil/mem_pool/Pool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/date_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DateAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    typedef SingleValueAttributeReader<uint32_t> DateAttributeReader;
    DateAttributeReaderTest();
    ~DateAttributeReaderTest();

    DECLARE_CLASS_NAME(DateAttributeReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRead();

private:
    void FullBuild(const std::vector<uint32_t>& docCounts, std::vector<std::string>& ans);

    void CreateDataForOneSegment(segmentid_t segId, uint32_t docCount, std::vector<std::string>& ans);

    void CheckRead(const DateAttributeReader& reader, const std::vector<std::string>& ans);

private:
    config::AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool _pool;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateAttributeReaderTest, TestRead);
}} // namespace indexlib::index

#endif //__INDEXLIB_DATEATTRIBUTEREADERTEST_H
