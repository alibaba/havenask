#include "aitheta_indexer/plugins/aitheta/test/raw_segment_test.h"

#include <fstream>

#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/test/schema_maker.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void RawSegmentTest::Test() {
    const auto &partDir = GET_PARTITION_DIRECTORY();
    string dir("raw_segment_test");
    DirectoryPtr dirPtr = partDir->MakeDirectory(dir);
    RawSegment segment;
    CategoryRecords records;
    segment.GetCatId2Records().insert({1, records});
    ASSERT_TRUE(segment.Dump(dirPtr));

    RawSegment other;
    ASSERT_TRUE(other.Load(dirPtr));
}

IE_NAMESPACE_END(aitheta_plugin);
