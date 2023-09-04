#ifndef __VAR_NUM_ATTRIBUTE_WRITER_UNITTEST_H_
#define __VAR_NUM_ATTRIBUTE_WRITER_UNITTEST_H_

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class VarNumAttributeWriterTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForUpdateField();
    void TestCaseForWrite();
    void TestCaseForAddEncodeField();
    void TestCaseForDumpPerf();
    void TestCaseForDumpPerfForUniqField();
    void TestCaseForDumpCompressedOffset();
    void TestCaseForDumpDataInfo();
    void TestCaseForFloatEncode();

private:
    template <typename T>
    VarNumAttributeWriter<T>* PrepareWriter(bool isUniq, util::BuildResourceMetrics* buildMetrics = NULL);

    template <typename T>
    void TestAddField(uint32_t docCount, uint32_t valueCountPerField, uint64_t offsetThreshold, int64_t offsetUnitSize);

    template <typename T>
    void InnerTestAddEncodeField(uint32_t uniqDocCount, uint32_t valueCountPerField, uint32_t loop,
                                 uint64_t offsetThreshold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                                 int64_t offsetUnitSize = sizeof(uint32_t));

    void InnerTestAddEncodeFloatField(uint32_t fixedValueCount, const std::string& compressTypeStr,
                                      uint64_t offsetThreshold, int64_t offsetUnitSize, uint32_t docCount,
                                      uint32_t loop);

    void InnerTestDumpCompressedOffset(bool isUpdatable, size_t expectLen);

    void PrepareData(std::vector<std::string>& fields, uint32_t uniqDocCount, uint32_t valueCountPerField,
                     uint32_t loop);

    void CheckDataInfo(file_system::DirectoryPtr dir, const std::string& attrName, uint32_t uniqItemCount,
                       uint32_t maxItemLen);

protected:
    std::string mRootDir;
    autil::mem_pool::Pool mPool;
};

//////////////////////////////////////////////////////////////
template <typename T>
void VarNumAttributeWriterTest::TestAddField(uint32_t docCount, uint32_t valueCountPerField, uint64_t offsetThreshold,
                                             int64_t offsetUnitSize)
{
    CaseTearDown();
    CaseSetUp();
    IndexTestUtil::ResetDir(mRootDir);
    config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
    attrConfig->SetU32OffsetThreshold(offsetThreshold);

    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    VarNumAttributeWriter<T> writer(attrConfig);
    util::BuildResourceMetrics buildMetrics;
    buildMetrics.Init();
    writer.Init(FSWriterParamDeciderPtr(), &buildMetrics);
    writer.SetAttrConvertor(convertor);

    std::vector<std::string> fields;
    PrepareData(fields, docCount, valueCountPerField, 0);
    size_t dataLen = 0;
    autil::mem_pool::Pool pool;
    int64_t factor = writer.InitDumpEstimateFactor();
    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = convertor->Encode(autil::StringView(fields[i]), &pool);
        writer.AddField((docid_t)i, encodeStr);
        dataLen += (valueCountPerField * sizeof(T) +
                    common::VarNumAttributeFormatter::GetEncodedCountLength(valueCountPerField));
        EXPECT_EQ(dataLen + (i + 1) * sizeof(uint64_t), buildMetrics.GetValue(util::BMT_DUMP_FILE_SIZE));
        EXPECT_EQ((i + 1) * factor, buildMetrics.GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE));
    }

    AttributeWriterHelper::DumpWriter(writer, mRootDir);

    // check data file
    std::string dataFilePath = mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    std::string offsetFilePath = mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
    fslib::FileMeta fileMeta;
    ASSERT_EQ(file_system::FSEC_OK, file_system::FslibWrapper::GetFileMeta(dataFilePath, fileMeta));
    EXPECT_EQ((int64_t)dataLen, fileMeta.fileLength);

    ASSERT_EQ(file_system::FSEC_OK, file_system::FslibWrapper::GetFileMeta(offsetFilePath, fileMeta));
    EXPECT_EQ(offsetUnitSize * (docCount + 1), fileMeta.fileLength);
}

template <typename T>
void VarNumAttributeWriterTest::InnerTestAddEncodeField(uint32_t uniqDocCount, uint32_t valueCountPerField,
                                                        uint32_t loop, uint64_t offsetThreshold, int64_t offsetUnitSize)
{
    IndexTestUtil::ResetDir(mRootDir);
    config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
    attrConfig->SetU32OffsetThreshold(offsetThreshold);
    ASSERT_TRUE(attrConfig->SetCompressType("uniq").IsOK());
    common::AttributeConvertor* convertor =
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig);
    VarNumAttributeWriter<T> writer(attrConfig);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    autil::mem_pool::Pool pool;

    writer.SetAttrConvertor(common::AttributeConvertorPtr(convertor));

    std::vector<std::string> fields;
    PrepareData(fields, uniqDocCount, valueCountPerField, loop);
    EXPECT_EQ(uniqDocCount * (loop + 1), fields.size());

    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = convertor->Encode(autil::StringView(fields[i]), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    uint32_t unitSize =
        common::VarNumAttributeFormatter::GetEncodedCountLength(valueCountPerField) + valueCountPerField * sizeof(T);
    AttributeWriterHelper::DumpWriter(writer, mRootDir);

    // check data file
    std::string dataFilePath = mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    std::string offsetFilePath = mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
    fslib::FileMeta fileMeta;
    ASSERT_EQ(file_system::FSEC_OK, file_system::FslibWrapper::GetFileMeta(dataFilePath, fileMeta));
    EXPECT_EQ((int64_t)uniqDocCount * unitSize, fileMeta.fileLength);

    ASSERT_EQ(file_system::FSEC_OK, file_system::FslibWrapper::GetFileMeta(offsetFilePath, fileMeta));
    EXPECT_EQ((int64_t)(offsetUnitSize * (fields.size() + 1)), fileMeta.fileLength);

    file_system::MemFileNodePtr reader(file_system::MemFileNodeCreator::TEST_Create());
    ASSERT_EQ(file_system::FSEC_OK, reader->Open("", offsetFilePath, file_system::FSOT_MEM, -1));
    ASSERT_EQ(file_system::FSEC_OK, reader->Populate());

    void* buffer = reader->GetBaseAddress();
    for (size_t i = 0; i < fields.size(); i++) {
        uint64_t value = 0;
        if (offsetUnitSize == sizeof(uint32_t)) {
            value = *((uint32_t*)buffer + i);
        } else {
            value = *((uint64_t*)buffer + i);
        }
        EXPECT_EQ(unitSize * (i % uniqDocCount), value);
    }
    ASSERT_EQ(file_system::FSEC_OK, reader->Close());
}

template <typename T>
VarNumAttributeWriter<T>* VarNumAttributeWriterTest::PrepareWriter(bool isUniq,
                                                                   util::BuildResourceMetrics* buildMetrics)
{
    config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
    if (isUniq) {
        EXPECT_TRUE(attrConfig->SetCompressType("uniq").IsOK());
    }
    // test udpate uniq field
    VarNumAttributeWriter<T>* writer = new VarNumAttributeWriter<T>(attrConfig);
    writer->Init(FSWriterParamDeciderPtr(), buildMetrics);

    common::AttributeConvertorPtr convertor = AttributeTestUtil::CreateAttributeConvertor(attrConfig);
    writer->SetAttrConvertor(convertor);
    return writer;
}
}} // namespace indexlib::index

#endif //__VAR_NUM_ATTRIBUTE_WRITER_UNITTEST_H_
