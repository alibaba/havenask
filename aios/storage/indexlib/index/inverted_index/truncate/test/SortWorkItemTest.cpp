// #include "indexlib/index/inverted_index/truncate/SortWorkItem.h"

// #include "indexlib/index/inverted_index/truncate/test/FakeTruncateAttributeReader.h"
// #include "unittest/unittest.h"

// namespace indexlib::index {

// class MockTrunAttrReaderCreator : public TruncateAttributeReaderCreator
// {
// public:
//     MockTrunAttrReaderCreator(const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema)
//         : TruncateAttributeReaderCreator(tabletSchema, indexlibv2::index::IIndexMerger::SegmentMergeInfos(),
//         std::make_shared<DocMapper>())
//     {
//     }
//     ~MockTrunAttrReaderCreator() {}
//     MOCK_METHOD(std::shared_ptr<TruncateAttributeReader>, Create, (const std::string&), (override));
// };

// class SortWorkItemTest : public TESTBASE
// {
// public:
//     void setUp() override {}
//     void tearDown() override {}
// };

// TEST_F(SortWorkItemTest, TestSimpleProcess)
// {
//     // prepare schema
//     std::string schemaStr = R"({
//         "attributes": [
//             "field1",
//             "field2"
//         ],
//         "fields": [
//             {
//                 "field_name": "field1",
//                 "field_type": "INTEGER"
//             },
//             {
//                 "field_name": "field2",
//                 "field_type": "INTEGER"
//             },
//         ],
//         "table_name": "test"
//     })";
//     auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
//     ASSERT_TRUE(tabletSchema->Deserialize(schemaStr));
//     MockTrunAttrReaderCreator* creator = new MockTrunAttrReaderCreator(tabletSchema);
//     uint32_t docCount = 10;
//     auto bucketMap = std::make_shared<BucketMap>();
//     bucketMap->Init(docCount);

//     indexlibv2::config::TruncateProfile profile;
//     indexlib::config::SortParam sortParam;
//     sortParam.SetSortField("field1");
//     sortParam.SetSortPattern(indexlibv2::config::sp_desc);
//     indexlib::config::SortParam sortParam2;
//     sortParam2.SetSortField("field2");
//     sortParam2.SetSortPattern(indexlibv2::config::sp_asc);
//     profile.sortParams.push_back(sortParam);
//     profile.sortParams.push_back(sortParam2);

//     SortWorkItem workItem(profile, docCount, creator, bucketMap, tabletSchema);
//     // FakeTruncateAttributeReader<int32_t>* fakeReader = new FakeTruncateAttributeReader<int32_t>();
//     auto fakeReader1 = std::make_shared<FakeTruncateAttributeReader<int32_t>>();
//     fakeReader1->SetAttrValue("1;-2;6;9;6;10;3;-8;-5;6",
//     "false;true;false;true;false;false;false;false;false;false"); auto fakeReader2 =
//     std::make_shared<FakeTruncateAttributeReader<int32_t>>(); fakeReader2->SetAttrValue("3;4;9;-2;0;3;-4;1;8;5",
//     "false;false;false;false;true;false;false;false;false;false");

//     // doc id:        5,9,2,4,     6,0, 8, 7, 1,      3
//     // field1 value: 10,6,6,6,     3,1,-5,-8,-2[null],9[null]
//     // field2 value:    5,9,0[null]
//     auto attrReader = std::dynamic_pointer_cast<TruncateAttributeReader>(fakeReader1);
//     auto attrReader2 = std::dynamic_pointer_cast<TruncateAttributeReader>(fakeReader2);
//     EXPECT_CALL(*creator, Create("field1")).WillOnce(Return(attrReader));
//     EXPECT_CALL(*creator, Create("field2")).WillOnce(Return(attrReader2));
//     workItem.process();
//     std::vector<docid_t> expectedDocIds = {5, 9, 2, 4, 6, 0, 8, 7, 1, 3};
//     for (uint32_t i = 0; i < docCount; ++i) {
//         ASSERT_EQ(expectedDocIds[i], workItem._docInfos[i].docId);
//     }
// }

// } // namespace indexlib::index
