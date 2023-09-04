#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib { namespace index_base {

class PartitionMetaTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PartitionMetaTest);
    void CaseSetUp() override
    {
        mDir = GET_TEMP_DATA_PATH();
        mPartMeta.AddSortDescription("field1", indexlibv2::config::sp_asc);
        mPartMeta.AddSortDescription("field2", indexlibv2::config::sp_asc);
        mPartMeta.AddSortDescription("field3", indexlibv2::config::sp_desc);
    }

    void CaseTearDown() override {}

    void TestCaseForStore()
    {
        index::IndexTestUtil::ResetDir(mDir);
        mPartMeta.Store(mDir, file_system::FenceContext::NoFence());
        std::string partMeta = mDir + INDEX_PARTITION_META_FILE_NAME;
        INDEXLIB_TEST_TRUE(file_system::FslibWrapper::IsExist(partMeta).GetOrThrow());
    }

    void TestCaseForLoad()
    {
        {
            mPartMeta.Store(mDir, file_system::FenceContext::NoFence());

            PartitionMeta partMeta;
            partMeta.Load(mDir);

            ASSERT_NO_THROW({ partMeta.AssertEqual(mPartMeta); });
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
        mPartMeta.Store(mDir, file_system::FenceContext::NoFence());
        PartitionMeta partMeta;
        partMeta.Load(mDir);
        indexlibv2::config::SortDescription desc = partMeta.GetSortDescription(0);
        INDEXLIB_TEST_EQUAL(desc.GetSortFieldName(), "field1");
        INDEXLIB_TEST_EQUAL(desc.GetSortPattern(), indexlibv2::config::sp_asc);

        desc = partMeta.GetSortDescription(1);
        INDEXLIB_TEST_EQUAL(desc.GetSortFieldName(), "field2");
        INDEXLIB_TEST_EQUAL(desc.GetSortPattern(), indexlibv2::config::sp_asc);

        desc = partMeta.GetSortDescription(2);
        INDEXLIB_TEST_EQUAL(desc.GetSortFieldName(), "field3");
        INDEXLIB_TEST_EQUAL(desc.GetSortPattern(), indexlibv2::config::sp_desc);
    }

    void TestCaseForAssertEqual()
    {
        mPartMeta.Store(mDir, file_system::FenceContext::NoFence());
        PartitionMeta partMeta;
        partMeta.Load(mDir);

        ASSERT_NO_THROW({ partMeta.AssertEqual(mPartMeta); });

        partMeta.AddSortDescription("field4", indexlibv2::config::sp_desc);
        ASSERT_THROW(partMeta.AssertEqual(mPartMeta), util::AssertEqualException);
    }

    void TestCaseForJsonizeSortDescription()
    {
        string jsonStr = "{"
                         "\"SortField\" : \"aa\","
                         "\"SortPattern\" : \"desc\""
                         "}";
        indexlibv2::config::SortDescription desc;
        FromJsonString(desc, jsonStr);
        INDEXLIB_TEST_EQUAL("aa", desc.GetSortFieldName());
        INDEXLIB_TEST_EQUAL(indexlibv2::config::sp_desc, desc.GetSortPattern());

        jsonStr = "{"
                  "\"sort_field\" : \"aa\","
                  "\"sort_pattern\" : \"desc\""
                  "}";
        indexlibv2::config::SortDescription desc2;
        FromJsonString(desc2, jsonStr);
        INDEXLIB_TEST_EQUAL("aa", desc2.GetSortFieldName());
        INDEXLIB_TEST_EQUAL(indexlibv2::config::sp_desc, desc2.GetSortPattern());

        jsonStr = "{"
                  "\"sort_field\" : \"aa\","
                  "\"sort_pattern\" : \"\""
                  "}";
        indexlibv2::config::SortDescription desc3;
        FromJsonString(desc3, jsonStr);
        INDEXLIB_TEST_EQUAL("aa", desc3.GetSortFieldName());
        INDEXLIB_TEST_EQUAL(indexlibv2::config::sp_desc, desc3.GetSortPattern());
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
}} // namespace indexlib::index_base
