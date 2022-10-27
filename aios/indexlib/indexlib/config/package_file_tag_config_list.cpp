#include "indexlib/config/package_file_tag_config_list.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackageFileTagConfigList);

PackageFileTagConfigList::PackageFileTagConfigList() 
{
}

PackageFileTagConfigList::~PackageFileTagConfigList() 
{
}

void PackageFileTagConfigList::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("package_file_tag_config", configs, configs);
}

void PackageFileTagConfigList::TEST_PATCH()
{
    // eg. segment_189_level_0/attribute/sku_default_c2c/187_155.patch
    string jsonStr = R"(
    {
        "package_file_tag_config":
        [
            {"file_patterns": ["_PATCH_"], "tag": "PATCH" }
        ]
    }
    )";

    FromJsonString(*this, jsonStr);
}

const string& PackageFileTagConfigList::Match(const string& relativeFilePath, const string& defaultTag) const
{
    for (const auto& config : configs)
    {
        if (config.Match(relativeFilePath))
        {
            return config.GetTag();
        }
    }
    return defaultTag;
}

IE_NAMESPACE_END(config);

