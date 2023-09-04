#pragma once

#include <memory>

#include "autil/Log.h"
#include "future_lite/Executor.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"

namespace indexlibv2::index {

class PkTestSchema : public config::TabletSchema
{
public:
    PkTestSchema() : TabletSchema() {}
    const std::string& GetTableType() const override { return _tableType; }
    std::vector<std::shared_ptr<config::IIndexConfig>> GetIndexConfigs() const override
    {
        _configs.clear();
        auto pkConfigs = config::ITabletSchema::GetIndexConfigs(PRIMARY_KEY_INDEX_TYPE_STR);
        _configs.insert(_configs.end(), pkConfigs.begin(), pkConfigs.end());
        auto deletionMapConfigs = config::ITabletSchema::GetIndexConfigs("deletionmap");
        _configs.insert(_configs.end(), deletionMapConfigs.begin(), deletionMapConfigs.end());
        return _configs;
    }

private:
    std::string _tableType = "normal";
    mutable std::vector<std::shared_ptr<config::IIndexConfig>> _configs;
};

class PrimaryKeyTabletOptions : public config::TabletOptions
{
public:
};

class PrimaryKeyReaderTesthelper
{
public:
    PrimaryKeyReaderTesthelper();
    ~PrimaryKeyReaderTesthelper();

public:
    template <typename Key>
    static std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
    CreatePrimaryKeyIndexConfig(const std::string& indexName, PrimaryKeyIndexType pkIndexType,
                                bool hasPKAttribute = true);

    template <typename Key>
    static std::shared_ptr<config::TabletSchema> CreatePrimaryKeyTabletSchema(bool hasPKAttribute = true);

    static void MakeFakePrimaryKeyDocStr(uint32_t urls[], uint32_t urlCount, std::string& docStr);

    static indexlib::file_system::IFileSystemPtr PrepareFileSystemForMMap(bool isLock, std::string outputRoot);
    static indexlib::file_system::IFileSystemPtr PrepareFileSystemForCache(const std::string& cacheType,
                                                                           size_t blockSize, std::string outputRoot);

    static indexlib::config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode
    GetLoadModeByPkIndexType(PrimaryKeyIndexType pkIndexType)
    {
        switch (pkIndexType) {
        case pk_hash_table:
            return indexlib::config::PrimaryKeyLoadStrategyParam::HASH_TABLE;
        case pk_sort_array:
            return indexlib::config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
        case pk_block_array:
            return indexlib::config::PrimaryKeyLoadStrategyParam::BLOCK_VECTOR;
        default:
            assert(false);
            return indexlib::config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
        }
    }

    static std::shared_ptr<framework::Tablet> CreateTablet(const std::string& indexRootPath,
                                                           const std::shared_ptr<config::TabletSchema>& schema,
                                                           versionid_t versionId, segmentid_t* segmentIds,
                                                           size_t segmentCount, future_lite::Executor* executor,
                                                           future_lite::TaskScheduler* taskScheduler)
    {
        framework::Version version(versionId);
        for (size_t i = 0; i < segmentCount; ++i) {
            version.AddSegment(segmentIds[i]);
        }
        indexlibv2::framework::TabletResource resource;
        resource.tabletId = indexlib::framework::TabletId("pkTest");
        resource.dumpExecutor = executor;
        resource.taskScheduler = taskScheduler;
        auto options = std::make_unique<PrimaryKeyTabletOptions>();
        options->SetFlushLocal(false);
        options->SetFlushRemote(true);
        auto tablet = std::make_shared<framework::Tablet>(resource);
        framework::IndexRoot indexRoot(indexRootPath, /*remoteRoot*/ indexRootPath);
        if (!tablet->Open(indexRoot, schema, std::move(options), version.GetVersionId()).IsOK()) {
            return nullptr;
        }
        return tablet;
    }

private:
    AUTIL_LOG_DECLARE();
};

template <typename Key>
std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
PrimaryKeyReaderTesthelper::CreatePrimaryKeyIndexConfig(const std::string& indexName, PrimaryKeyIndexType pkIndexType,
                                                        bool hasPKAttribute)
{
    ::InvertedIndexType indexType;
    if (typeid(Key) == typeid(uint64_t)) {
        indexType = ::it_primarykey64;

    } else {
        indexType = ::it_primarykey128;
    }

    auto pkConfig = std::make_shared<indexlibv2::index::PrimaryKeyIndexConfig>(indexName, indexType);
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>(indexName, ft_string, false);
    auto status = pkConfig->SetFieldConfig(fieldConfig);
    assert(status.IsOK());
    pkConfig->SetPrimaryKeyAttributeFlag(hasPKAttribute);
    pkConfig->SetPrimaryKeyIndexType(pkIndexType);
    auto loadMode = GetLoadModeByPkIndexType(pkIndexType);
    status = pkConfig->SetPrimaryKeyLoadParam(loadMode);
    assert(status.IsOK());
    return pkConfig;
}

template <typename Key>
std::shared_ptr<config::TabletSchema> PrimaryKeyReaderTesthelper::CreatePrimaryKeyTabletSchema(bool hasPKAttribute)
{
    std::string pkField;
    if (typeid(Key) == typeid(uint64_t)) {
        pkField = "pk:primarykey64:url";

    } else {
        pkField = "pk:primarykey128:url";
    }
    auto schema = table::NormalTabletSchemaMaker::Make("url:string;",   // above is field schema
                                                       pkField.c_str(), // above is index schema
                                                       "",              // above is attribute schema
                                                       "");             // above is summary schema

    std::dynamic_pointer_cast<PrimaryKeyIndexConfig>(schema->GetIndexConfig(PRIMARY_KEY_INDEX_TYPE_STR, "pk"))
        ->SetPrimaryKeyAttributeFlag(hasPKAttribute);
    return schema;
}

inline void PrimaryKeyReaderTesthelper::MakeFakePrimaryKeyDocStr(uint32_t urls[], uint32_t urlCount,
                                                                 std::string& docStr)
{
    docStr.clear();
    for (uint32_t i = 0; i < urlCount; ++i) {
        std::stringstream ss;
        ss << "{[url: (url" << urls[i] << ")]};";
        docStr.append(ss.str());
    }
}

} // namespace indexlibv2::index
