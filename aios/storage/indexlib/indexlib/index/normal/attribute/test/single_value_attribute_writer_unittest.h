#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SingleValueAttributeWriterTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForUInt32AddField();
    void TestCaseForUInt32UpdateField();
    void TestCaseForDumpPerf();
    void TestAddNullField();
    void TestCaseUpdateBuildResourceMetrics();

private:
    template <typename T>
    void AddData(SingleValueAttributeWriter<T>& writer, T expectedData[], uint32_t docNum,
                 const common::AttributeConvertorPtr& convertor, util::BuildResourceMetrics& buildMetrics);

    template <typename T>
    void UpdateData(SingleValueAttributeWriter<T>& writer, T expectedData[], uint32_t docNum,
                    const common::AttributeConvertorPtr& convertor, util::BuildResourceMetrics& buildMetrics);

    template <typename T>
    void TestAddField();

    template <typename T>
    void TestUpdateField();

    template <typename T>
    void CheckWriterData(const SingleValueAttributeWriter<T>& writer, T expectedData[], uint32_t docNum);

    void CheckDataFile(uint32_t docNum, const std::string& dataFilePath, size_t typeSize);

protected:
    std::string mDir;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void SingleValueAttributeWriterTest::TestAddField()
{
    config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>();
    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    SingleValueAttributeWriter<T> writer(attrConfig);
    util::BuildResourceMetrics buildMetrics;
    buildMetrics.Init();
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), &buildMetrics);

    static const uint32_t DOC_NUM = 100;
    T expectedData[DOC_NUM];
    AddData(writer, expectedData, DOC_NUM, convertor, buildMetrics);
    CheckWriterData(writer, expectedData, DOC_NUM);
    AttributeWriterHelper::DumpWriter(writer, mDir);
    // check data file
    std::string dataFilePath = attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    CheckDataFile(DOC_NUM, dataFilePath, sizeof(T));
}

template <typename T>
void SingleValueAttributeWriterTest::TestUpdateField()
{
    config::AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>();
    common::AttributeConvertorPtr convertor(
        common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    SingleValueAttributeWriter<T> writer(attrConfig);
    util::BuildResourceMetrics buildMetrics;
    buildMetrics.Init();
    writer.SetAttrConvertor(convertor);
    writer.Init(FSWriterParamDeciderPtr(), &buildMetrics);

    static const uint32_t DOC_NUM = 100;
    T expectedData[DOC_NUM];
    AddData(writer, expectedData, DOC_NUM, convertor, buildMetrics);
    UpdateData(writer, expectedData, DOC_NUM, convertor, buildMetrics);
    CheckWriterData(writer, expectedData, DOC_NUM);
}

template <typename T>
void SingleValueAttributeWriterTest::CheckWriterData(const SingleValueAttributeWriter<T>& writer, T expectedData[],
                                                     uint32_t docNum)
{
    TypedSliceList<T>* data = static_cast<TypedSliceList<T>*>(writer.mFormatter->GetAttributeData());
    for (uint32_t i = 0; i < docNum; ++i) {
        // assert(expectedData[i] == (*sliceArray)[i]);
        T value;
        data->Read(i, value);
        INDEXLIB_TEST_EQUAL(expectedData[i], value);
    }
}
}} // namespace indexlib::index
