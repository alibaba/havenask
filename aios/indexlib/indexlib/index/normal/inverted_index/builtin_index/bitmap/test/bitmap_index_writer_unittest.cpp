#include <map>
#include <vector>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/test/version_maker.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

const string INDEX_NAME_TAG = "bitmap";

class BitmapIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    typedef BitmapPostingMaker::Key2DocIdListMap Key2DocIdListMap;

    BitmapIndexWriterTest()
    {
    }

    ~BitmapIndexWriterTest()
    {
    }

    DECLARE_CLASS_NAME(BitmapIndexWriterTest);

    void CaseSetUp() override
    {
        mTestDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOneDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 1;
        docid_t baseDocId = 0;
        BitmapPostingMaker maker(mTestDir, INDEX_NAME_TAG);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, answer);
        Version version = VersionMaker::Make(0, "0");
        version.Store(mTestDir, false);

        CheckData(answer);
    }

    void TestCaseForMultiDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 10;
        docid_t baseDocId = 0;
        BitmapPostingMaker maker(mTestDir, INDEX_NAME_TAG);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, answer);
        Version version = VersionMaker::Make(0, "0");
        version.Store(mTestDir, false);

        CheckData(answer);
    }


private:
    void CheckData(const Key2DocIdListMap& answer)
    {
        string fieldName = INDEX_NAME_TAG;
        string field = fieldName+":text;";
        string index = INDEX_NAME_TAG + ":text:" + fieldName;
        IndexPartitionSchemaPtr schema = 
            SchemaMaker::MakeSchema(field, index, "", "");
        INDEXLIB_TEST_TRUE(schema);
        IndexConfigPtr indexConfig = 
            schema->GetIndexSchema()->GetIndexConfig(INDEX_NAME_TAG);

        index_base::PartitionDataPtr partitionData = 
            test::PartitionDataMaker::CreateSimplePartitionData(
                    GET_FILE_SYSTEM(), mTestDir);

        BitmapIndexReader reader;
        INDEXLIB_TEST_TRUE(reader.Open(indexConfig, partitionData));
        
        Key2DocIdListMap::const_iterator it;
        for (it = answer.begin(); it != answer.end(); ++it)
        {
            Term term(it->first, INDEX_NAME_TAG);
            PostingIteratorPtr postIt(reader.Lookup(term));
            INDEXLIB_TEST_TRUE(postIt != NULL);
        }
    }

    string mTestDir;
    static const uint32_t MAX_DOC_COUNT = 1000;
};

INDEXLIB_UNIT_TEST_CASE(BitmapIndexWriterTest, TestCaseForOneDoc);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexWriterTest, TestCaseForMultiDoc);

IE_NAMESPACE_END(index);
