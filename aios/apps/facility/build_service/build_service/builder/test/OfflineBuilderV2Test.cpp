#include "build_service/builder/OfflineBuilderV2.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/builder/BuilderV2Impl.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/util/ParallelIdGenerator.h"
#include "future_lite/Executor.h"
#include "future_lite/ExecutorCreator.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletCreator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/document_creator.h"
#include "unittest/unittest.h"

namespace build_service::builder {

class OfflineBuilderV2Test : public BUILD_SERVICE_TESTBASE
{
public:
    void setUpKV();
    void setUpNormal();

protected:
    config::ResourceReaderPtr _resourceReader;
    std::string _indexRoot;
    proto::PartitionId _partId;
};

void OfflineBuilderV2Test::setUpKV()
{
    auto configPath = GET_TEST_DATA_PATH() + "/offline_builder_v2_kv_test/config";
    _resourceReader = std::make_shared<config::ResourceReader>(configPath);
    _indexRoot = GET_TEMP_DATA_PATH();
    proto::BuildId buildId;
    buildId.set_appname("test_kv");
    buildId.set_generationid(1);
    *_partId.add_clusternames() = "simple";
    *_partId.mutable_buildid() = buildId;
}

void OfflineBuilderV2Test::setUpNormal()
{
    auto configPath = GET_TEST_DATA_PATH() + "/offline_builder_v2_normal_test/config";
    _resourceReader = std::make_shared<config::ResourceReader>(configPath);
    _indexRoot = GET_TEMP_DATA_PATH();
    proto::BuildId buildId;
    buildId.set_appname("test_normal");
    buildId.set_generationid(1);
    *_partId.add_clusternames() = "simple";
    *_partId.mutable_buildid() = buildId;
}

// build private version
class FakeOfflineBuilderV2 : public OfflineBuilderV2
{
    FakeOfflineBuilderV2(const std::string& clusterName, const proto::PartitionId& partitionId,
                         const config::ResourceReaderPtr& resourceReader, const std::string& indexRoot)
        : OfflineBuilderV2(clusterName, partitionId, resourceReader, indexRoot)
    {
    }
    bool initWithOpenVersionId(const config::BuilderConfig& builderConfig,
                               std::shared_ptr<indexlib::util::MetricProvider> metricProvider,
                               versionid_t openVersionId)
    {
        auto tabletOptions = _resourceReader->getTabletOptions(_clusterName);
        if (!tabletOptions) {
            BS_LOG(ERROR, "read tablet options for %s failed", _clusterName.c_str());
            return false;
        }
        tabletOptions->SetIsLeader(true);
        tabletOptions->SetFlushRemote(true);
        tabletOptions->SetFlushLocal(false);
        tabletOptions->SetIsOnline(false);

        _executor = future_lite::ExecutorCreator::Create(
            /*type*/ "async_io",
            future_lite::ExecutorCreator::Parameters()
                .SetExecutorName("tablet_dump" + autil::StringUtil::toString(_partitionId.range().from()) + "_" +
                                 autil::StringUtil::toString(_partitionId.range().to()))
                .SetThreadNum(tabletOptions->GetOfflineConfig().GetBuildConfig().GetDumpThreadCount()));

        _taskScheduler = std::make_unique<future_lite::TaskScheduler>(_executor.get());

        auto schema = _resourceReader->getTabletSchema(_clusterName);
        if (!schema) {
            BS_LOG(ERROR, "read tablet schema for %s failed", _clusterName.c_str());
            return false;
        }

        auto idGenerator = std::make_shared<util::ParallelIdGenerator>(indexlibv2::IdMaskType::BUILD_PUBLIC,
                                                                       indexlibv2::IdMaskType::BUILD_PRIVATE);
        auto status = idGenerator->Init(0, 2);
        auto tablet = indexlibv2::framework::TabletCreator()
                          .SetTabletId(indexlib::framework::TabletId(_clusterName))
                          .SetExecutor(_executor.get(), _taskScheduler.get())
                          .SetIdGenerator(idGenerator)
                          .CreateTablet();
        if (!tablet) {
            BS_LOG(ERROR, "create tablet failed for cluster %s", _clusterName.c_str());
            return false;
        }

        indexlibv2::framework::IndexRoot indexRoot(_indexRoot, _indexRoot);
        auto s = tablet->Open(indexRoot, schema, std::move(tabletOptions), openVersionId);
        if (!s.IsOK()) {
            BS_LOG(ERROR, "open tablet for cluster[%s] with indexRoot[%s] failed, error: %s", _clusterName.c_str(),
                   _indexRoot.c_str(), s.ToString().c_str());
            return false;
        }
        _impl = std::make_unique<BuilderV2Impl>(std::move(tablet), _buildId);
        if (!_impl->init(builderConfig, std::move(metricProvider))) {
            BS_LOG(ERROR, "init builder impl failed for cluster: %s", _clusterName.c_str());
            return false;
        }
        return true;
    }
};

TEST_F(OfflineBuilderV2Test, testBuildKVTable)
{
    setUpKV();
    auto builder = std::make_unique<OfflineBuilderV2>("simple", _partId, _resourceReader, _indexRoot);
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder->init(builderConfig, nullptr));
    ASSERT_TRUE(builder->_impl.get() != nullptr);

    auto schema = _resourceReader->getTabletSchema("simple");
    ASSERT_TRUE(schema);
    std::string docStr = "cmd=add,key=1,value1=11,value2=111,ts=1,locator=0:1;"
                         "cmd=add,key=2,value1=22,value2=222,ts=2,locator=0:2;"
                         "cmd=add,key=3,value1=33,value2=333,ts=3,locator=0:3;";
    auto kvDocs = indexlib::test::DocumentCreator::CreateKVDocuments(schema->GetLegacySchema(), docStr);
    for (const auto& kvDoc : kvDocs) {
        auto kvDocBatch = std::make_shared<indexlibv2::document::KVDocumentBatch>();
        auto pool = kvDocBatch->GetPool();
        const auto& kvIndexDoc = *(kvDoc->begin());
        auto docInfo = kvDoc->GetDocInfo();
        auto kvDocV2 = kvIndexDoc.CreateKVDocumentV2(pool, docInfo, kvDoc->GetLocatorV2());
        kvDocBatch->AddDocument(std::move(kvDocV2));
        ASSERT_TRUE(builder->build(std::move(kvDocBatch)));
    }
    auto impl = dynamic_cast<BuilderV2Impl*>(builder->_impl.get());
    ASSERT_TRUE(impl != nullptr);
    builder->stop(1024, true, false);
    ASSERT_TRUE(builder->isSealed());
    ASSERT_FALSE(builder->hasFatalError());

    std::vector<std::string> fileList;
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(_indexRoot);
    auto status = indexlibv2::framework::VersionLoader::ListVersion(indexDir, &fileList);
    ASSERT_TRUE(!fileList.empty());

    versionid_t maxVersionId = indexlibv2::INVALID_VERSIONID;
    for (const std::string& fileName : fileList) {
        versionid_t versionId = indexlibv2::framework::VersionLoader::GetVersionId(fileName);
        maxVersionId = std::max(maxVersionId, versionId);
    }
    ASSERT_NE(indexlibv2::INVALID_VERSIONID, maxVersionId);
    // load latest version & check locator
    auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(_indexRoot);
    auto [st, version] = indexlibv2::framework::VersionLoader::GetVersion(rootDir, maxVersionId);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(version->GetLocator().DebugString(), builder->getLastLocator().DebugString());
    ASSERT_EQ(version->GetLocator(), builder->getLastLocator());
}

TEST_F(OfflineBuilderV2Test, testBuildKVTableWithPrivateVersion)
{
    setUpKV();
    auto builder = std::make_unique<FakeOfflineBuilderV2>("simple", _partId, _resourceReader, _indexRoot);
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder->initWithOpenVersionId(builderConfig, nullptr, indexlibv2::INVALID_VERSIONID));
    ASSERT_TRUE(builder->_impl.get() != nullptr);

    auto schema = _resourceReader->getTabletSchema("simple");
    ASSERT_TRUE(schema);
    std::string docStr = "cmd=add,key=1,value1=11,value2=111,ts=1,locator=0:1;"
                         "cmd=add,key=2,value1=22,value2=222,ts=2,locator=0:2;"
                         "cmd=add,key=3,value1=33,value2=333,ts=3,locator=0:3;";
    auto rawDocs = indexlibv2::document::RawDocumentMaker::MakeBatch(docStr);
    for (const auto& rawDoc : rawDocs) {
        auto kvDocBatch = indexlibv2::document::KVDocumentBatchMaker::Make(schema, {rawDoc});
        ASSERT_TRUE(kvDocBatch);
        ASSERT_TRUE(builder->build(std::move(kvDocBatch)));
    }
    auto impl = dynamic_cast<BuilderV2Impl*>(builder->_impl.get());
    ASSERT_TRUE(impl != nullptr);
    builder->stop(1024, true, false);
    ASSERT_TRUE(builder->isSealed());
    ASSERT_FALSE(builder->hasFatalError());

    std::vector<std::string> fileList;
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(_indexRoot);
    auto status = indexlibv2::framework::VersionLoader::ListVersion(indexDir, &fileList);
    ASSERT_TRUE(!fileList.empty());

    versionid_t maxVersionId = indexlibv2::INVALID_VERSIONID;
    for (const std::string& fileName : fileList) {
        versionid_t versionId = indexlibv2::framework::VersionLoader::GetVersionId(fileName);
        maxVersionId = std::max(maxVersionId, versionId);
    }
    ASSERT_NE(indexlibv2::INVALID_VERSIONID, maxVersionId);
    // load latest version & check locator
    auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(_indexRoot);
    auto [st, version] = indexlibv2::framework::VersionLoader::GetVersion(rootDir, maxVersionId);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(version->GetLocator().DebugString(), builder->getLastLocator().DebugString());
    ASSERT_EQ(version->GetLocator(), builder->getLastLocator());

    // failover
    builder = std::make_unique<FakeOfflineBuilderV2>("simple", _partId, _resourceReader, _indexRoot);
    ASSERT_TRUE(builder->initWithOpenVersionId(builderConfig, nullptr, version->GetVersionId()));
    ASSERT_TRUE(builder->_impl.get() != nullptr);
    ASSERT_EQ(version->GetLocator().DebugString(), builder->getLastLocator().DebugString());
    ASSERT_EQ(version->GetLocator(), builder->getLastLocator());
}

TEST_F(OfflineBuilderV2Test, testNormalTableParallelBuild)
{
    setUpNormal();
    auto builder = std::make_unique<OfflineBuilderV2>(/*clusterName=*/"simple", _partId, _resourceReader, _indexRoot);
    config::BuilderConfig builderConfig;
    builderConfig.inconsistentModeBuildThreadCount = 2;
    builderConfig.consistentModeBuildThreadCount = 2;
    ASSERT_TRUE(builder->init(builderConfig, nullptr));
    ASSERT_TRUE(builder->_impl.get() != nullptr);

    auto schema = _resourceReader->getTabletSchema("simple");
    ASSERT_NE(schema, nullptr);
    std::string docStr =
        "cmd=add,pk=1,string1=hello,ts=1,locator=0:1;cmd=add,pk=2,string1=world,ts=2,locator=0:2;cmd=add,pk=3,string1="
        "hello,ts=3,locator=0:3;cmd=add,pk=4,string1=world,ts=4,locator=0:4;";

    std::vector<indexlib::document::NormalDocumentPtr> docs =
        indexlib::test::DocumentCreator::CreateNormalDocuments(schema->GetLegacySchema(), docStr);
    for (const auto& doc : docs) {
        auto docBatch = std::make_shared<indexlibv2::document::DocumentBatch>();
        docBatch->AddDocument(doc);
        ASSERT_TRUE(builder->build(std::move(docBatch)));
    }
    auto impl = dynamic_cast<BuilderV2Impl*>(builder->_impl.get());
    ASSERT_NE(impl, nullptr);
    builder->stop(/*stopTimestamp=*/1024, /*needSeal=*/true, /*immediately=*/false);
    ASSERT_TRUE(builder->isSealed());
    ASSERT_FALSE(builder->hasFatalError());
}

} // namespace build_service::builder
