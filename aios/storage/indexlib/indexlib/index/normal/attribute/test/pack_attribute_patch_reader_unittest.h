#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_patch_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PackAttributePatchReaderTest : public INDEXLIB_TESTBASE
{
public:
    PackAttributePatchReaderTest();
    ~PackAttributePatchReaderTest();

    DECLARE_CLASS_NAME(PackAttributePatchReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckReader(PackAttributePatchReaderPtr& packAttrPatchReader,
                     const config::PackAttributeConfigPtr& packAttrConfig, docid_t expectedDocId,
                     const std::string& expectStr);

    void CheckReaderForSeek(const index_base::PartitionDataPtr& partitionData,
                            const config::PackAttributeConfigPtr& packAttrConfig, docid_t expectedDocId,
                            const std::string& expectStr);

    void InnerCheckPatchValue(uint8_t* buffer, size_t dataLen, const std::string& expectStr);

private:
    uint8_t* mBuffer;
    size_t mBufferLen;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributePatchReaderTest, TestSimpleProcess);
}} // namespace indexlib::index
