#include "indexlib/merger/test/pack_table_merge_meta_unittest.h"

#include "autil/EnvUtil.h"
#include "indexlib/merger/table_merge_meta.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PackTableMergeMetaTest);

INDEXLIB_UNIT_TEST_CASE(PackTableMergeMetaTest, TestSimpleProcess);

PackTableMergeMetaTest::PackTableMergeMetaTest() {}

PackTableMergeMetaTest::~PackTableMergeMetaTest() {}

void PackTableMergeMetaTest::CaseSetUp() { EnvUtil::setEnv("INDEXLIB_PACKAGE_MERGE_META", "true"); }

void PackTableMergeMetaTest::CaseTearDown() { EnvUtil::unsetEnv("INDEXLIB_PACKAGE_MERGE_META"); }
}} // namespace indexlib::merger
