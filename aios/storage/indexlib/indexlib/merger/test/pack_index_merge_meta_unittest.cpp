#include "indexlib/merger/test/pack_index_merge_meta_unittest.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PackIndexMergeMetaTest);
INDEXLIB_UNIT_TEST_CASE(PackIndexMergeMetaTest, TestLoadLegacyMeta);

PackIndexMergeMetaTest::PackIndexMergeMetaTest() {}

PackIndexMergeMetaTest::~PackIndexMergeMetaTest() {}

void PackIndexMergeMetaTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH() + "/merge_meta";
    EnvUtil::setEnv("INDEXLIB_PACKAGE_MERGE_META", "true");
}

void PackIndexMergeMetaTest::CaseTearDown() { EnvUtil::unsetEnv("INDEXLIB_PACKAGE_MERGE_META"); }
}} // namespace indexlib::merger
