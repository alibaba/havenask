#ifndef __INDEXLIB_PACKATTRIBUTEITERATORTYPEDTEST_H
#define __INDEXLIB_PACKATTRIBUTEITERATORTYPEDTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PackAttributeIteratorTypedTest : public INDEXLIB_TESTBASE
{
public:
    PackAttributeIteratorTypedTest();
    ~PackAttributeIteratorTypedTest();

    DECLARE_CLASS_NAME(PackAttributeIteratorTypedTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    template <typename T>
    void CheckSingleIterator(const PackAttributeReaderPtr& packAttrReader, const std::string& attrName, T expectValue);
    template <typename T>
    void CheckMultiIterator(const PackAttributeReaderPtr& packAttrReader, const std::string& attrName,
                            const std::string& expectValue);

private:
    std::string mRootDir;
    std::string mDoc;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributeIteratorTypedTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_PACKATTRIBUTEITERATORTYPEDTEST_H
