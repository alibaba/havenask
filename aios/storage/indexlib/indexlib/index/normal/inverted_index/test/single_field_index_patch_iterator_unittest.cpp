// 迁移到：SingleFieldIndexSegmentPatchIteratorTest.cpp
#include <map>
#include <vector>

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/single_field_index_patch_iterator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SingleFieldIndexPatchIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SingleFieldIndexPatchIteratorTest() {}
    ~SingleFieldIndexPatchIteratorTest() {}

    DECLARE_CLASS_NAME(SingleFieldIndexPatchIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override {}
    void TestSimpleProcess();
    void TestMultiSegment();

private:
    void PrepareSegmentData(test::PartitionStateMachine* psm,
                            const std::vector<std::pair<std::string, int64_t>>& segment);
    void CheckPatchIterator(SingleFieldIndexPatchIterator* iter,
                            const std::map<uint64_t, std::map<segmentid_t, std::vector<docid_t>>>& expectedResult);

private:
    std::string _rootDir;
    config::IndexPartitionSchemaPtr _schema;
    config::IndexPartitionOptions _options;
    std::unordered_map<std::string, int64_t> _datas;

private:
    IE_LOG_DECLARE();
};

void SingleFieldIndexPatchIteratorTest::PrepareSegmentData(test::PartitionStateMachine* psm,
                                                           const std::vector<std::pair<std::string, int64_t>>& segment)
{
    std::string docStr;
    for (const auto& oneDoc : segment) {
        const std::string& pk = oneDoc.first;
        int64_t price = oneDoc.second;
        docStr = docStr + "cmd=add,price=" + autil::StringUtil::toString(price) + ",pk=" + pk + ",ts=1";
        if (_datas.find(pk) != _datas.end()) {
            docStr = docStr + ",modify_fields=price,modify_values=" + autil::StringUtil::toString(_datas[pk]);
        }
        _datas[pk] = price;
        docStr += ";";
    }
    psm->Transfer(test::BUILD_INC_NO_MERGE, docStr, "", "");
}

void SingleFieldIndexPatchIteratorTest::CheckPatchIterator(
    SingleFieldIndexPatchIterator* iter,
    const std::map<uint64_t, std::map<segmentid_t, std::vector<docid_t>>>& expectedResult)
{
    std::map<uint64_t, std::map<segmentid_t, std::vector<docid_t>>> actualResult;
    while (true) {
        std::unique_ptr<IndexUpdateTermIterator> termIter = iter->Next();
        if (termIter == nullptr) {
            break;
        }
        auto termKey = termIter->GetTermKey();
        segmentid_t segmentId = termIter->GetSegmentId();
        ComplexDocId complexDocId;
        while (termIter->Next(&complexDocId)) {
            docid_t docId;
            if (complexDocId.IsDelete()) {
                docId = -complexDocId.DocId();
            } else {
                docId = complexDocId.DocId();
            }
            assert(!termKey.IsNull());
            actualResult[termKey.GetKey()][segmentId].push_back(docId);
        }
    }
    EXPECT_EQ(expectedResult, actualResult);
}

void SingleFieldIndexPatchIteratorTest::CaseSetUp()
{
    _datas.clear();
    std::string field = "price:int64;pk:string";
    std::string index = "price:number:price;pk:primarykey64:pk";
    std::string attribute = "pk;price";
    _schema = test::SchemaMaker::MakeSchema(field, index, attribute, "");
    _schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    _schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    _rootDir = GET_TEMP_DATA_PATH();
    _options = config::IndexPartitionOptions();
    _options.SetEnablePackageFile(false);
}

void SingleFieldIndexPatchIteratorTest::TestSimpleProcess()
{
    test::PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(_schema, _options, _rootDir));

    PrepareSegmentData(&psm, {{"a", 100}, {"b", 100}, {"c", 200}});
    PrepareSegmentData(&psm, {{"c", 100}});

    const auto& indexConfig = _schema->GetIndexSchema()->GetIndexConfig("price");
    const auto& partitionData = psm.GetIndexPartition()->GetPartitionData();

    SingleFieldIndexPatchIterator iter(indexConfig, false);
    iter.Init(partitionData, false, INVALID_SEGMENTID);

    CheckPatchIterator(&iter, {
                                  {/*termKey=*/100, {{/*segmentId=*/0, {2}}}},
                                  {/*termKey=*/200, {{/*segmentId=*/0, {-2}}}},
                              });
}

void SingleFieldIndexPatchIteratorTest::TestMultiSegment()
{
    test::PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(_schema, _options, _rootDir));

    PrepareSegmentData(&psm, {{"a", 100}, {"b", 100}, {"c", 200}});
    PrepareSegmentData(&psm, {{"d", 100}, {"e", 100}, {"f", 200}});
    PrepareSegmentData(&psm, {{"c", 100}, {"e", 200}});
    PrepareSegmentData(&psm, {{"b", 200}, {"f", 100}});

    const auto& indexConfig = _schema->GetIndexSchema()->GetIndexConfig("price");
    const auto& partitionData = psm.GetIndexPartition()->GetPartitionData();

    SingleFieldIndexPatchIterator iter(indexConfig, false);
    iter.Init(partitionData, false, INVALID_SEGMENTID);

    CheckPatchIterator(&iter, {
                                  {
                                      /*termKey=*/100,
                                      {
                                          {/*segmentId=*/0, {-1, 2}},
                                          {/*segmentId=*/1, {-1, 2}},
                                      },
                                  },
                                  {
                                      /*termKey=*/200,
                                      {
                                          {/*segmentId=*/0, {1, -2}},
                                          {/*segmentId=*/1, {1, -2}},
                                      },
                                  },
                              });
}

INDEXLIB_UNIT_TEST_CASE(SingleFieldIndexPatchIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SingleFieldIndexPatchIteratorTest, TestMultiSegment);
}} // namespace indexlib::index
