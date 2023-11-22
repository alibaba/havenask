#pragma once
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace index {

class VarNumAttributeSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    VarNumAttributeSegmentReaderTest() {}
    ~VarNumAttributeSegmentReaderTest() {}

public:
    DECLARE_CLASS_NAME(VarNumAttributeSegmentReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForWriteRead();
    void TestCaseForWriteReadWithSmallOffsetThreshold();
    void TestCaseForWriteReadWithUniqEncode();
    void TestCaseForWriteReadWithUniqEncodeAndSmallThreshold();
    void TestCaseForWriteReadWithCompress();
    void TestCaseForWriteReadWithSmallOffsetThresholdWithCompress();
    void TestCaseForWriteReadWithUniqEncodeWithCompress();
    void TestCaseForWriteReadWithUniqEncodeAndSmallThresholdWithCompress();

private:
    void CheckOffsetFileLength(std::string& offsetFilePath, int64_t targetLength);

private:
    template <typename T>
    void TestWriteAndRead(uint64_t offsetThreshold, uint64_t offsetUnitSize, bool uniqEncode = false,
                          bool needEqualCompress = false);
};

///////////////////////////////////////////////////////////////////////////
template <typename T>
void VarNumAttributeSegmentReaderTest::TestWriteAndRead(uint64_t offsetThreshold, uint64_t offsetUnitSize,
                                                        bool uniqEncode, bool needEqualCompress)
{
    tearDown();
    setUp();
    config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
    std::string compressTypeStr = config::CompressTypeOption::GetCompressStr(
        uniqEncode, needEqualCompress, attrConfig->GetCompressType().HasPatchCompress(), /*blockFpEncode*/ false,
        /*fp16Encode*/ false,
        /*int8Encode*/ false, /*int8Abs*/ 0.0f);

    ASSERT_TRUE(attrConfig->SetCompressType(compressTypeStr).IsOK());
    attrConfig->SetU32OffsetThreshold(offsetThreshold);

    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    VarNumAttributeWriter<T> writer(attrConfig);
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    autil::mem_pool::Pool pool;
    static const uint32_t DOC_NUM = 100;

    std::vector<std::vector<T>> data;
    std::string lastDocStr;

    for (uint32_t i = 0; i < DOC_NUM; ++i) {
        // make duplicate attribute doc
        if (i % 2 == 1) {
            data.push_back(*data.rbegin());
            autil::StringView encodeStr = convertor->Encode(autil::StringView(lastDocStr), &pool);
            writer.AddField((docid_t)i, encodeStr);
            continue;
        }
        uint32_t valueLen = i * 3 % 10;
        std::stringstream ss;
        std::vector<T> dataOneDoc;
        for (uint32_t j = 0; j < valueLen; j++) {
            if (j != 0) {
                ss << MULTI_VALUE_SEPARATOR;
            }

            int value = (i + j) * 3 % 128;
            ss << value;
            dataOneDoc.push_back((T)value);
        }
        data.push_back(dataOneDoc);
        lastDocStr = ss.str();
        autil::StringView encodeStr = convertor->Encode(autil::StringView(lastDocStr), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    file_system::DirectoryPtr attrDirectory = GET_SEGMENT_DIRECTORY()->MakeDirectory(ATTRIBUTE_DIR_NAME);
    util::SimplePool dumpPool;
    writer.Dump(attrDirectory, &dumpPool);

    file_system::IFileSystemPtr fs = GET_FILE_SYSTEM();
    fs->Sync(true).GetOrThrow();

    std::string offsetFilePath = util::PathUtil::JoinPath(attrDirectory->GetLogicalPath(), attrConfig->GetAttrName(),
                                                          ATTRIBUTE_OFFSET_FILE_NAME);

    if (!needEqualCompress) {
        CheckOffsetFileLength(offsetFilePath, offsetUnitSize * (DOC_NUM + 1));
    }

    MultiValueAttributeSegmentReader<T> segReader(attrConfig);
    index_base::SegmentInfo segInfo;
    segInfo.docCount = DOC_NUM;

    file_system::DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
    index_base::SegmentData segData = IndexTestUtil::CreateSegmentData(segDirectory, segInfo, 0, 0);
    segReader.Open(segData, PatchApplyOption::NoPatch(), nullptr, nullptr, false);

    for (uint32_t i = 0; i < DOC_NUM; ++i) {
        autil::MultiValueType<T> multiValue;
        bool isNull;
        auto ctx = segReader.CreateReadContext(nullptr);
        INDEXLIB_TEST_TRUE(segReader.Read((docid_t)i, multiValue, isNull, ctx));
        INDEXLIB_TEST_EQUAL(data[i].size(), multiValue.size());
        for (size_t j = 0; j < data[i].size(); j++) {
            INDEXLIB_TEST_EQUAL(data[i][j], multiValue[j]);
        }
    }

    for (uint32_t i = 0; i < DOC_NUM; ++i) {
        autil::MultiValueType<T> multiValue;
        bool isNull;
        auto ctx = segReader.CreateReadContext(nullptr);
        INDEXLIB_TEST_TRUE(segReader.Read((docid_t)i, multiValue, isNull, ctx));
        INDEXLIB_TEST_EQUAL(data[i].size(), multiValue.size());
        for (size_t j = 0; j < data[i].size(); j++) {
            INDEXLIB_TEST_EQUAL(data[i][j], multiValue[j]);
        }
    }

    if (!needEqualCompress) {
        CheckOffsetFileLength(offsetFilePath, offsetUnitSize * (DOC_NUM + 1));
    }
}
}} // namespace indexlib::index
