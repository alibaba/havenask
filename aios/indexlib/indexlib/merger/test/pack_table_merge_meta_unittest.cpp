#include "indexlib/merger/test/pack_table_merge_meta_unittest.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, PackTableMergeMetaTest);

INDEXLIB_UNIT_TEST_CASE(PackTableMergeMetaTest, TestSimpleProcess);
    
PackTableMergeMetaTest::PackTableMergeMetaTest()
{
}

PackTableMergeMetaTest::~PackTableMergeMetaTest()
{
}

void PackTableMergeMetaTest::CaseSetUp()
{
    EnvUtil::SetEnv("INDEXLIB_PACKAGE_MERGE_META", "true");
}

void PackTableMergeMetaTest::CaseTearDown()
{
    EnvUtil::UnsetEnv("INDEXLIB_PACKAGE_MERGE_META");
}

IE_NAMESPACE_END(merger);

