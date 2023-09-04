#ifndef __INDEXLIB_SPATIALATTRIBUTETEST_H
#define __INDEXLIB_SPATIALATTRIBUTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
namespace indexlib { namespace index {

class SpatialAttributeTest : public INDEXLIB_TESTBASE
{
public:
    SpatialAttributeTest();
    ~SpatialAttributeTest();

    DECLARE_CLASS_NAME(SpatialAttributeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLocation();

private:
    void InnerTestLocation(bool isUniqEncode);
    void CheckValues(const file_system::DirectoryPtr& rootDir, const AttributeReaderPtr& attrReader,
                     const std::string& expectValueString, bool isAttrUpdatable);
    uint64_t GetUniqCount(const std::string& attrName, const std::string& segmentId);
    IE_LOG_DECLARE();

private:
    std::string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(SpatialAttributeTest, TestLocation);
}} // namespace indexlib::index

#endif //__INDEXLIB_SPATIALATTRIBUTETEST_H
