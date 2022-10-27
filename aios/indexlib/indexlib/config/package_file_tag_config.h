#ifndef __INDEXLIB_PACKAGE_FILE_TAG_CONFIG_H
#define __INDEXLIB_PACKAGE_FILE_TAG_CONFIG_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/regular_expression.h"

IE_NAMESPACE_BEGIN(config);

class PackageFileTagConfig : public autil::legacy::Jsonizable
{
public:
    PackageFileTagConfig();
    ~PackageFileTagConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Match(const std::string& filePath) const;
    const std::string& GetTag() const { return mTag; }

private:
    void Init();

private:
    static const std::string& BuiltInPatternToRegexPattern(const std::string& builtInPattern);

private:
    misc::RegularExpressionVector mRegexVec;
    std::vector<std::string> mFilePatterns;
    std::string mTag;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileTagConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACKAGE_FILE_TAG_CONFIG_H
