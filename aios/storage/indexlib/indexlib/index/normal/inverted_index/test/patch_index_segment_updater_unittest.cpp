// 迁移到：InvertedIndexSegmentUpdaterTest.cpp
#include <sstream>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"
#include "indexlib/index/inverted_index/patch/IndexPatchFileReader.h"
#include "indexlib/index/normal/inverted_index/accessor/patch_index_segment_updater.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;

namespace indexlib { namespace index {

#define FIELD_NAME_TAG "price"

class PatchIndexSegmentUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 1024;
    void CaseSetUp() override {}
    void CaseTearDown() override {}

    void TestSimple();
};

namespace {

void Update(PatchIndexSegmentUpdater& updater, uint64_t termKey, const std::vector<docid_t> docIds)
{
    for (docid_t docId : docIds) {
        document::ModifiedTokens tokens(0);
        if (docId < 0) {
            tokens.Push(document::ModifiedTokens::Operation::REMOVE, termKey);
            docId = -docId;
        } else {
            tokens.Push(document::ModifiedTokens::Operation::ADD, termKey);
        }
        updater.Update(docId, tokens);
    }
}

void Check(const file_system::DirectoryPtr& dir,
           const std::vector<std::pair<uint64_t, std::vector<docid_t>>>& expectedResults)
{
    IndexPatchFileReader patchFileReader(/*srcSegmentid_t=*/9, /*targetSegmentId=*/0, /*patchCompress=*/false);
    ASSERT_TRUE(patchFileReader.Open(dir->GetIDirectory(), "9_0.patch").IsOK());

    index::DictKeyInfo lastTerm;
    std::vector<docid_t>* lastVector = nullptr;
    std::vector<std::pair<uint64_t, std::vector<docid_t>>> actualResults;
    while (patchFileReader.HasNext()) {
        patchFileReader.Next();
        auto curTerm = patchFileReader.GetCurrentTermKey();
        ComplexDocId complexDocId = patchFileReader.GetCurrentDocId();
        docid_t docId;
        if (complexDocId.IsDelete()) {
            docId = -complexDocId.DocId();
        } else {
            docId = complexDocId.DocId();
        }
        if (curTerm == lastTerm and lastVector != nullptr) {
            lastVector->push_back(docId);
        } else {
            lastTerm = curTerm;
            assert(!curTerm.IsNull());
            actualResults.push_back({curTerm.GetKey(), {docId}});
            lastVector = &actualResults.back().second;
        }
    }
    EXPECT_EQ(expectedResults, actualResults);
}

} // namespace

void PatchIndexSegmentUpdaterTest::TestSimple()
{
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("test", it_number_int64));
    PatchIndexSegmentUpdater updater(nullptr, 0, indexConfig);
    Update(updater, 100, {-3, 1, 3, 0, 4, 3, -1}); // 0, -1, 3, 4
    Update(updater, 200, {-3, 1, -1});             // -1, -3
    Update(updater, 300, {-3, 1, 9});              // 1, -3, 9
    Update(updater, 400, {0});                     // 0
    Update(updater, 500, {1, 2});
    Update(updater, 600, {1, 1});

    auto indexDir = GET_PARTITION_DIRECTORY();
    updater.Dump(indexDir, 9);

    auto dir = indexDir->GetDirectory("test", true);

    ASSERT_TRUE(dir->IsExist("9_0.patch"));

    Check(dir, {
                   {100, {0, -1, 3, 4}},
                   {200, {-1, -3}},
                   {300, {1, -3, 9}},
                   {400, {0}},
                   {500, {1, 2}},
                   {600, {1}},
               });
}

INDEXLIB_UNIT_TEST_CASE(PatchIndexSegmentUpdaterTest, TestSimple);
}} // namespace indexlib::index
