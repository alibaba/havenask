#ifndef __INDEXLIB_PACKAGE_FILE_TAG_CONFIG_LIST_H
#define __INDEXLIB_PACKAGE_FILE_TAG_CONFIG_LIST_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/package_file_tag_config.h"

IE_NAMESPACE_BEGIN(config);

class PackageFileTagConfigList : public autil::legacy::Jsonizable
{
public:
    PackageFileTagConfigList();
    ~PackageFileTagConfigList();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& Match(const std::string& relativeFilePath, const std::string& defaultTag) const;

private:
    void TEST_PATCH();

private:
    std::vector<PackageFileTagConfig> configs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileTagConfigList);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACKAGE_FILE_TAG_CONFIG_LIST_H
