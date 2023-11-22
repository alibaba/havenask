#include <map>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::test;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {

const string INDEX_NAME_TAG = "bitmap";

class BitmapIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    typedef BitmapPostingMaker::Key2DocIdListMap Key2DocIdListMap;

    BitmapIndexWriterTest() {}

    ~BitmapIndexWriterTest() {}

    DECLARE_CLASS_NAME(BitmapIndexWriterTest);

    void CaseSetUp() override { mTestDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void TestCaseForOneDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 1;
        docid_t baseDocId = 0;
        BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), INDEX_NAME_TAG, /*enableNullTerm=*/false);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, &answer);
        Version version = VersionMaker::Make(0, "0");
        version.Store(GET_PARTITION_DIRECTORY(), false);

        CheckData(answer);
    }

    void TestCaseForMultiDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 10;
        docid_t baseDocId = 0;
        BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), INDEX_NAME_TAG, /*enableNullTerm=*/false);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, &answer);
        Version version = VersionMaker::Make(0, "0");
        version.Store(GET_PARTITION_DIRECTORY(), false);

        CheckData(answer);
    }

private:
    void CheckData(const Key2DocIdListMap& answer)
    {
        string fieldName = INDEX_NAME_TAG;
        string field = fieldName + ":text;";
        string index = INDEX_NAME_TAG + ":text:" + fieldName;
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
        INDEXLIB_TEST_TRUE(schema);
        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig(INDEX_NAME_TAG);

        index_base::PartitionDataPtr partitionData =
            test::PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM(), "");

        BitmapIndexReader reader;
        INDEXLIB_TEST_TRUE(reader.Open(indexConfig, partitionData));

        Key2DocIdListMap::const_iterator it;
        for (it = answer.begin(); it != answer.end(); ++it) {
            Term term(it->first, INDEX_NAME_TAG);
            std::shared_ptr<PostingIterator> postIt(reader.Lookup(term, 1000, nullptr, nullptr).ValueOrThrow());
            INDEXLIB_TEST_TRUE(postIt != NULL);
        }
    }

    string mTestDir;
    static const uint32_t MAX_DOC_COUNT = 1000;
};

INDEXLIB_UNIT_TEST_CASE(BitmapIndexWriterTest, TestCaseForOneDoc);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexWriterTest, TestCaseForMultiDoc);
}}} // namespace indexlib::index::legacy
