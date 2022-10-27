#include <ha3/sql/ops/khronosScan/test/KhronosTestTableBuilder.h>
#include <ha3/test/test.h>
#include <indexlib/util/memory_control/memory_quota_controller.h>
#include <indexlib/util/task_scheduler.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/util/memory_control/memory_quota_controller.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using autil::mem_pool::Pool;
using namespace navi;
using namespace khronos::search;
using namespace khronos::indexlib_plugin;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, KhronosTestTableBuilder);

#define RUN_AND_CHECK(condition)                \
    if (!condition) {                           \
        SQL_LOG(ERROR, #condition " failed");   \
        return false;                           \
    }


KhronosTestTableBuilder::KhronosTestTableBuilder()
    : _buildCallFlag(false)
    , _buildSuccessFlag(false)
{
}

KhronosTestTableBuilder::~KhronosTestTableBuilder() {
}

bool KhronosTestTableBuilder::getBinaryTsData(KhronosTestUtil::BinaryTsDataList &out)
{
    out = {
        {"cpu;app=ha3,host=10.10.10.1;", {{{1}, 5.0}, {{2}, 5.1}, {{4}, 5.2}, {{8}, 5.1}, {{10}, 5.2}, {{12}, 5.2}}},
        {"cpu;app=bs,host=10.10.10.1;", {{{9}, 6.0}, {{10}, 6.0}, {{13}, 6.0}}},
        {"cpu;app=bs,host=10.10.10.2;", {{{20}, 6.0}, {{22}, 6.0}, {{25}, 6.0}}},
        {"mem;app=ha3,host=10.10.10.3;", {{{16}, 6.0}}},
        {"mem;app=ha3,host=10.10.10.1;", {{{20}, 6.0}, {{25}, 6.0}, {{28}, 6.0}, {{40}, 5.2}, {{44}, 5.2}}},
        {"mem;cluster=c2,host=10.10.10.1;", {{{20}, 6.0}, {{41}, 6.0}, {{44}, 6.0}, {{49}, 6.0}, {{50}, 6.0}, {{55}, 5.2}}},
        {"mem;cluster=c3,host=10.10.10.2;", {{{22}, 6.0}, {{30}, 6.0}, {{38}, 6.0}, {{40}, 6.0}, {{41}, 6.0}, {{50}, 5.2}}},
        {"mem;cluster=c4,host=10.10.10.3;", {{{43}, 6.0}, {{46}, 6.0}, {{48}, 6.0}, {{50}, 6.0}, {{52}, 6.0}, {{58}, 5.2}}}
    };
    return true;
}

bool KhronosTestTableBuilder::build(const std::string &dataRootDir) {
    KhronosTestUtil::BinaryTsDataList binaryTsDataList;
    RUN_AND_CHECK(getBinaryTsData(binaryTsDataList));
    return build(dataRootDir, binaryTsDataList);
}

bool KhronosTestTableBuilder::build(const std::string &dataRootDir,
                                    const KhronosTestUtil::BinaryTsDataList &binaryTsDataList)
{
    assert(!_buildCallFlag);
    _buildCallFlag = true;

    const string libKhronosTableSoDir = string(INSTALL_ROOT) + "/usr/local/lib64/";
    const string schemaPath = string(TEST_DATA_PATH) + "/khronos/khronos_table_schema.json";

    _schema = SchemaAdapter::LoadSchema("", schemaPath);
    _options.SetEnablePackageFile(false);
    _options.SetIsOnline(false);

    _partitionResource.indexPluginPath = libKhronosTableSoDir;
    _partitionResource.taskScheduler.reset(new TaskScheduler());
    _partitionResource.memoryQuotaController.reset(new MemoryQuotaController(4096L*1024*1024));

    _psm.reset(new PartitionStateMachine(DEFAULT_MEMORY_USE_LIMIT, false, _partitionResource));
    _options.GetBuildConfig(false).maxDocCount = 15;
    auto& mergeConfig = _options.GetMergeConfig();
    mergeConfig.mergeThreadCount = 3;
    mergeConfig.mergeStrategyParameter.outputLimitParam =
        "level1_time_stride_in_second=10;level1_watermark_latency_in_second=10;";

    RUN_AND_CHECK(_psm->Init(_schema, _options, dataRootDir));
    _psm->SetPsmOption("documentFormat", "binary");

    std::string fullDocs = KhronosTestUtil::PrepareTsDocumentInBinaryStringWithTsDesc(
            binaryTsDataList, &_pool, KhronosTestUtil::TimeMode::USE_KHRONOSTIME);

    RUN_AND_CHECK(_psm->Transfer(BUILD_FULL, fullDocs, "", ""));

    JoinRelationMap joinMap;
    _indexApp.reset(new IndexApplication());
    auto indexPartition = _psm->GetIndexPartition();
    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(indexPartition);
    if (!_indexApp->Init(indexPartitions, joinMap)) {
        AUTIL_LOG(ERROR, "psm transfer failed");
        return false;
    }
    return true;

    _buildSuccessFlag = true;
    return true;
}

END_HA3_NAMESPACE(sql);
