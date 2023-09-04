#ifndef __INDEXLIB_MULTIFIELDINDEXREADERTEST_H
#define __INDEXLIB_MULTIFIELDINDEXREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index { namespace legacy {

class MultiFieldIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    MultiFieldIndexReaderTest();
    ~MultiFieldIndexReaderTest();

    DECLARE_CLASS_NAME(MultiFieldIndexReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddIndexSegmentReader();
    void TestGetIndexReaderWithIndexId();

private:
    // indexName[:truncName1,truncIndex2];indexName;...
    MultiFieldIndexReaderPtr CreateReader(const std::string& readerStr);

    void CheckReaderExist(const MultiFieldIndexReaderPtr& reader, const std::string& indexName, fieldid_t fieldId);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiFieldIndexReaderTest, TestGetIndexReaderWithIndexId);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_MULTIFIELDINDEXREADERTEST_H
