#include "indexlib/plugin/test/index_plugin_loader_unittest.h"

#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/impl/offline_config_impl.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace plugin {
IE_LOG_SETUP(plugin, IndexPluginLoaderTest);

IndexPluginLoaderTest::IndexPluginLoaderTest() {}

IndexPluginLoaderTest::~IndexPluginLoaderTest() {}

void IndexPluginLoaderTest::CaseSetUp() {}

void IndexPluginLoaderTest::CaseTearDown() {}

void IndexPluginLoaderTest::TestSimpleProcess()
{
    string pluginRootPath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "plugin_loader_test");
    FslibWrapper::MkDirE(pluginRootPath, true, true);
    ASSERT_EQ(FSEC_OK,
              FslibWrapper::AtomicStore(util::PathUtil::JoinPath(pluginRootPath, "libdemo0.so"), "", true).Code());
    ASSERT_EQ(FSEC_OK,
              FslibWrapper::AtomicStore(util::PathUtil::JoinPath(pluginRootPath, "libdemo1.so"), "", true).Code());
    ASSERT_EQ(FSEC_OK,
              FslibWrapper::AtomicStore(util::PathUtil::JoinPath(pluginRootPath, "libsplit.so"), "", true).Code());
    IndexSchemaPtr indexSchema(new IndexSchema());
    FieldConfigPtr fieldConfig(new FieldConfig());
    fieldConfig->SetFieldType(ft_raw);
    fieldConfig->SetFieldId(0);
    {
        auto customizedIndexConfig = new CustomizedIndexConfig("demo0_index");
        customizedIndexConfig->SetIndexer("demo0");
        customizedIndexConfig->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    {
        auto customizedIndexConfig = new CustomizedIndexConfig("demo1_index");
        customizedIndexConfig->SetIndexer("demo1");
        customizedIndexConfig->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    ModuleInfo info;
    info.moduleName = "testsplit";
    info.modulePath = "libsplit.so";
    options.GetOfflineConfig().mImpl->mModuleInfos.push_back(info);
    auto manager = IndexPluginLoader::Load(pluginRootPath, indexSchema, options);
    ASSERT_TRUE(manager);
    auto loadedInfo = manager->getModuleInfo("testsplit");
    EXPECT_EQ(info.moduleName, loadedInfo.moduleName);
    EXPECT_EQ(util::PathUtil::JoinPath(pluginRootPath, info.modulePath), loadedInfo.modulePath);
    {
        // test two customized index share one Indexer
        auto customizedIndexConfig = new CustomizedIndexConfig("cindex");
        customizedIndexConfig->SetIndexer("demo1");
        customizedIndexConfig->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    ASSERT_TRUE(IndexPluginLoader::Load(pluginRootPath, indexSchema, options));
    {
        // non exist plugin
        auto customizedIndexConfig = new CustomizedIndexConfig("demo2_index");
        customizedIndexConfig->SetIndexer("demo2");
        customizedIndexConfig->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    // always load success, as pluginRootPath may pass from env
    ASSERT_TRUE(IndexPluginLoader::Load(pluginRootPath, indexSchema, options));
}

// void IndexPluginLoaderTest::TestLoadOfflineModules() {
//     string pluginRootPath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(),
//             "plugin_loader_test");
//     FslibWrapper::MkDirE(pluginRootPath, true, true);
//     ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(util::PathUtil::JoinPath(
//                     pluginRootPath, "libdemo0.so"), "", true));

//     IndexPartitionOptions options;
//     ModuleInfo info;
//     info.moduleName = "test";
//     info.modulePath = "libdemo0.so";
//     options.GetOfflineConfig().mImpl->mModuleInfos.push_back(info);
// }

}} // namespace indexlib::plugin
