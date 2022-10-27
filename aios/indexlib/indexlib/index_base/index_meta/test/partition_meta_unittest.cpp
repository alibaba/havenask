#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);

class PartitionMetaTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PartitionMetaTest);
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
        mPartMeta.AddSortDescription("field1", sp_asc);
        mPartMeta.AddSortDescription("field2", sp_asc);
        mPartMeta.AddSortDescription("field3", sp_desc);
    }

    void CaseTearDown() override
    {}

    void TestCaseForStore()
    {
        index::IndexTestUtil::ResetDir(mDir);
        mPartMeta.Store(mDir);
        std::string partMeta = mDir + INDEX_PARTITION_META_FILE_NAME;
        INDEXLIB_TEST_TRUE(storage::FileSystemWrapper::IsExist(partMeta));
    }

    void TestCaseForLoad()
    {
        {
            mPartMeta.Store(mDir);

            PartitionMeta partMeta;
            partMeta.Load(mDir);

            ASSERT_NO_THROW({
                        partMeta.AssertEqual(mPartMeta);
                            });
        }
        {
            index::IndexTestUtil::ResetDir(mDir);
            PartitionMeta partMeta;
            partMeta.Load(mDir);
            INDEXLIB_TEST_EQUAL((size_t)0, partMeta.Size());
        }
    }

    void TestCaseForGetSortDescription()
    {
        mPartMeta.Store(mDir);
        PartitionMeta partMeta;
        partMeta.Load(mDir);
        SortDescription desc = partMeta.GetSortDescription(0);
        INDEXLIB_TEST_EQUAL(desc.sortFieldName, "field1");
        INDEXLIB_TEST_EQUAL(desc.sortPattern, sp_asc);

        desc = partMeta.GetSortDescription(1);
        INDEXLIB_TEST_EQUAL(desc.sortFieldName, "field2");
        INDEXLIB_TEST_EQUAL(desc.sortPattern, sp_asc);

        desc = partMeta.GetSortDescription(2);
        INDEXLIB_TEST_EQUAL(desc.sortFieldName, "field3");
        INDEXLIB_TEST_EQUAL(desc.sortPattern, sp_desc);
    }

    void TestCaseForAssertEqual()
    {
        mPartMeta.Store(mDir);
        PartitionMeta partMeta;
        partMeta.Load(mDir);

        ASSERT_NO_THROW({partMeta.AssertEqual(mPartMeta);});

        partMeta.AddSortDescription("field4", sp_desc);
        ASSERT_THROW(partMeta.AssertEqual(mPartMeta), misc::AssertEqualException);
    }

    void TestCaseForJsonizeSortDescription()
    {
        string jsonStr = "{"
                         "\"SortField\" : \"aa\","
                         "\"SortPattern\" : \"desc\""
                         "}";
        SortDescription desc;
        FromJsonString(desc, jsonStr);
        INDEXLIB_TEST_EQUAL("aa", desc.sortFieldName);
        INDEXLIB_TEST_EQUAL(sp_desc, desc.sortPattern);

        jsonStr = "{"
                  "\"sort_field\" : \"aa\","
                  "\"sort_pattern\" : \"desc\""
                  "}";
        SortDescription desc2;
        FromJsonString(desc2, jsonStr);
        INDEXLIB_TEST_EQUAL("aa", desc2.sortFieldName);
        INDEXLIB_TEST_EQUAL(sp_desc, desc2.sortPattern);

        jsonStr = "{"
                  "\"sort_field\" : \"aa\","
                  "\"sort_pattern\" : \"\""
                  "}";
        SortDescription desc3;
        FromJsonString(desc3, jsonStr);
        INDEXLIB_TEST_EQUAL("aa", desc3.sortFieldName);
        INDEXLIB_TEST_EQUAL(sp_desc, desc3.sortPattern);
    }

private:
    std::string mDir;
    PartitionMeta mPartMeta;

    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index_base, PartitionMetaTest);

INDEXLIB_UNIT_TEST_CASE(PartitionMetaTest, TestCaseForStore);
INDEXLIB_UNIT_TEST_CASE(PartitionMetaTest, TestCaseForLoad);
INDEXLIB_UNIT_TEST_CASE(PartitionMetaTest, TestCaseForGetSortDescription);
INDEXLIB_UNIT_TEST_CASE(PartitionMetaTest, TestCaseForAssertEqual);
INDEXLIB_UNIT_TEST_CASE(PartitionMetaTest, TestCaseForJsonizeSortDescription);

IE_NAMESPACE_END(index_base);
