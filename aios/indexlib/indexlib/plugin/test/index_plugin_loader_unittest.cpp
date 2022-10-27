#include "indexlib/plugin/test/index_plugin_loader_unittest.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/impl/customized_index_config_impl.h"
#include "indexlib/config/impl/offline_config_impl.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(plugin);
IE_LOG_SETUP(plugin, IndexPluginLoaderTest);

IndexPluginLoaderTest::IndexPluginLoaderTest()
{
}

IndexPluginLoaderTest::~IndexPluginLoaderTest()
{
}

void IndexPluginLoaderTest::CaseSetUp()
{
}

void IndexPluginLoaderTest::CaseTearDown()
{
}

void IndexPluginLoaderTest::TestSimpleProcess()
{
    string pluginRootPath = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(),
            "plugin_loader_test");
    FileSystemWrapper::MkDirIfNotExist(pluginRootPath);
    FileSystemWrapper::AtomicStoreIgnoreExist(FileSystemWrapper::JoinPath(
                    pluginRootPath, "libdemo0.so"), "");
    FileSystemWrapper::AtomicStoreIgnoreExist(FileSystemWrapper::JoinPath(
                    pluginRootPath, "libdemo1.so"), "");
    FileSystemWrapper::AtomicStoreIgnoreExist(FileSystemWrapper::JoinPath(
                    pluginRootPath, "libsplit.so"), "");
    IndexSchemaPtr indexSchema(new IndexSchema());
    FieldConfigPtr fieldConfig(new FieldConfig());
    fieldConfig->SetFieldType(ft_raw);
    fieldConfig->SetFieldId(0);
    {
        auto customizedIndexConfig = new CustomizedIndexConfig("demo0_index",
                it_customized);
        customizedIndexConfig->mImpl->mIndexerName = "demo0";
        customizedIndexConfig->mImpl->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    {
        auto customizedIndexConfig = new CustomizedIndexConfig("demo1_index",
                it_customized);
        customizedIndexConfig->mImpl->mIndexerName = "demo1";
        customizedIndexConfig->mImpl->AddFieldConfig(fieldConfig);
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
    EXPECT_EQ(FileSystemWrapper::JoinPath(pluginRootPath, info.modulePath), loadedInfo.modulePath);
    {
        // test two customized index share one Indexer
        auto customizedIndexConfig = new CustomizedIndexConfig("cindex",
                it_customized);
        customizedIndexConfig->mImpl->mIndexerName = "demo1";
        customizedIndexConfig->mImpl->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    ASSERT_TRUE(IndexPluginLoader::Load(pluginRootPath, indexSchema, options));    
    {
        // non exist plugin
        auto customizedIndexConfig = new CustomizedIndexConfig("demo2_index",
                it_customized);
        customizedIndexConfig->mImpl->mIndexerName = "demo2";
        customizedIndexConfig->mImpl->AddFieldConfig(fieldConfig);
        IndexConfigPtr indexConfig(customizedIndexConfig);
        indexSchema->AddIndexConfig(indexConfig);
    }
    // always load success, as pluginRootPath may pass from env
    ASSERT_TRUE(IndexPluginLoader::Load(pluginRootPath, indexSchema, options));
}

// void IndexPluginLoaderTest::TestLoadOfflineModules() {
//     string pluginRootPath = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(),
//             "plugin_loader_test");
//     FileSystemWrapper::MkDirIfNotExist(pluginRootPath);
//     FileSystemWrapper::AtomicStoreIgnoreExist(FileSystemWrapper::JoinPath(
//                     pluginRootPath, "libdemo0.so"), "");

//     IndexPartitionOptions options;
//     ModuleInfo info;
//     info.moduleName = "test";
//     info.modulePath = "libdemo0.so";
//     options.GetOfflineConfig().mImpl->mModuleInfos.push_back(info);
// }

IE_NAMESPACE_END(plugin);

