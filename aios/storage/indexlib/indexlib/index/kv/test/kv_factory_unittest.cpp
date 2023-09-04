#include "indexlib/index/kv/test/kv_factory_unittest.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_common.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/index/kv/kv_segment_iterator.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvFactoryTest);

KvFactoryTest::KvFactoryTest() {}

KvFactoryTest::~KvFactoryTest() {}

void KvFactoryTest::CaseSetUp() {}

void KvFactoryTest::CaseTearDown() {}
}} // namespace indexlib::index
