#ifndef __INDEXLIB_LOAD_CONFIG_H
#define __INDEXLIB_LOAD_CONFIG_H

#include <memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/load_config_base.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/file_system/load_config/warmup_strategy.h"
#include "indexlib/misc/regular_expression.h"

DECLARE_REFERENCE_CLASS(file_system, LoadConfigImpl);

IE_NAMESPACE_BEGIN(file_system);

class LoadConfig : public LoadConfigBase
{
public:
    using LoadConfigBase::FilePatternStringVector;
    using LoadConfigBase::LoadMode;

public:
    LoadConfig();
    LoadConfig(const LoadConfig& other);
    ~LoadConfig();

public:
    void Check() const;
    bool EqualWith(const LoadConfig& other) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    LoadConfig& operator=(const LoadConfig& other);

public:
    bool Match(const std::string& filePath, const std::string& lifecycle) const;

private:
    std::unique_ptr<LoadConfigImpl> mImpl;

private:
    IE_LOG_DECLARE();

private:
    friend class LoadConfigTest;
};

DEFINE_SHARED_PTR(LoadConfig);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOAD_CONFIG_H
