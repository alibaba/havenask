#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/sort_work_item.h"
#include "indexlib/index/merger_util/truncate/test/fake_truncate_attribute_reader.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib::index::legacy {

class MockTrunAttrReaderCreator : public TruncateAttributeReaderCreator
{
public:
    MockTrunAttrReaderCreator(const config::AttributeSchemaPtr& attrSchema)
        : TruncateAttributeReaderCreator(SegmentDirectoryBasePtr(), attrSchema, index_base::SegmentMergeInfos(),
                                         ReclaimMapPtr())
    {
    }
    ~MockTrunAttrReaderCreator() {}

public:
    MOCK_METHOD(TruncateAttributeReaderPtr, Create, (const std::string&), (override));
};
typedef ::testing::NiceMock<MockTrunAttrReaderCreator> NiceMockTrunAttrReaderCreator;

class SortWorkItemTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SortWorkItemTest);
    void CaseSetUp() override
    {
        // TODO add case for random sort
    }
    void CaseTearDown() override {}

    void TestSimpleProcess()
    {
        AttributeSchemaPtr attrSchema(new AttributeSchema());
        AttributeConfigPtr attrConfig(new AttributeConfig());
        FieldConfigPtr fieldConfig(new FieldConfig("field1", FieldType::ft_int32, false));
        attrConfig->Init(fieldConfig);
        attrSchema->AddAttributeConfig(attrConfig);
        AttributeConfigPtr attrConfig2(new AttributeConfig());
        FieldConfigPtr fieldConfig2(new FieldConfig("field2", FieldType::ft_int32, false));
        attrConfig2->Init(fieldConfig2);
        attrSchema->AddAttributeConfig(attrConfig2);

        MockTrunAttrReaderCreator* creator = new NiceMockTrunAttrReaderCreator(attrSchema);
        uint32_t docCount = 10;
        BucketMapPtr bucketMap(new BucketMap());
        bucketMap->Init(docCount);

        TruncateProfile profile;
        SortParam sortParam;
        sortParam.SetSortField("field1");
        sortParam.SetSortPattern(indexlibv2::config::sp_desc);
        SortParam sortParam2;
        sortParam2.SetSortField("field2");
        sortParam2.SetSortPattern(indexlibv2::config::sp_asc);
        profile.mSortParams.push_back(sortParam);
        profile.mSortParams.push_back(sortParam2);

        SortWorkItem workItem(profile, docCount, creator, bucketMap);

        FakeTruncateAttributeReader<int32_t>* fakeReader = new FakeTruncateAttributeReader<int32_t>();
        fakeReader->SetAttrValue("1;-2;6;9;6;10;3;-8;-5;6",
                                 "false;true;false;true;false;false;false;false;false;false");
        FakeTruncateAttributeReader<int32_t>* fakeReader2 = new FakeTruncateAttributeReader<int32_t>();
        fakeReader2->SetAttrValue("3;4;9;-2;0;3;-4;1;8;5",
                                  "false;false;false;false;true;false;false;false;false;false");

        // doc id:        5,9,2,4,     6,0, 8, 7, 1,      3
        // field1 value: 10,6,6,6,     3,1,-5,-8,-2[null],9[null]
        // field2 value:    5,9,0[null]
        TruncateAttributeReaderPtr attrReader(fakeReader);
        TruncateAttributeReaderPtr attrReader2(fakeReader2);
        EXPECT_CALL(*creator, Create("field1")).WillOnce(Return(attrReader));
        EXPECT_CALL(*creator, Create("field2")).WillOnce(Return(attrReader2));
        workItem.Process();
        vector<docid_t> expectedDocIds = {5, 9, 2, 4, 6, 0, 8, 7, 1, 3};
        for (uint32_t i = 0; i < docCount; i++) {
            ASSERT_EQ(expectedDocIds[i], workItem.mDocInfos[i].mDocId);
        }

        delete creator;
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SortWorkItemTest);

INDEXLIB_UNIT_TEST_CASE(SortWorkItemTest, TestSimpleProcess);
} // namespace indexlib::index::legacy
