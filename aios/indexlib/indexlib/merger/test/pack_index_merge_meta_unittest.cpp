#include <autil/StringUtil.h>
#include "indexlib/merger/test/pack_index_merge_meta_unittest.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, PackIndexMergeMetaTest);

INDEXLIB_UNIT_TEST_CASE(PackIndexMergeMetaTest, TestStoreAndLoad);
INDEXLIB_UNIT_TEST_CASE(PackIndexMergeMetaTest, TestLoadAndStoreWithMergeResource);
INDEXLIB_UNIT_TEST_CASE(PackIndexMergeMetaTest, TestLoadLegacyMeta);

PackIndexMergeMetaTest::PackIndexMergeMetaTest()
{
}

PackIndexMergeMetaTest::~PackIndexMergeMetaTest()
{
}

void PackIndexMergeMetaTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH() + "/merge_meta";
    EnvUtil::SetEnv("INDEXLIB_PACKAGE_MERGE_META", "true");
}

void PackIndexMergeMetaTest::CaseTearDown()
{
    EnvUtil::UnsetEnv("INDEXLIB_PACKAGE_MERGE_META");
}

IE_NAMESPACE_END(merger);

