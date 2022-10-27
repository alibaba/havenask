#ifndef __INDEXLIB_VERSIONED_PACKAGE_FILE_META_H
#define __INDEXLIB_VERSIONED_PACKAGE_FILE_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/package_file_meta.h"

IE_NAMESPACE_BEGIN(file_system);

class VersionedPackageFileMeta : public PackageFileMeta
{
public:
    VersionedPackageFileMeta(int32_t version = 0);
    ~VersionedPackageFileMeta();
public:
    void Store(const std::string& root, const std::string& description);
    void Load(const std::string& metaFilePath);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    int32_t GetVersionId() { return mVersionId; }

public:
    static void Recognize(const std::string& description, int32_t recoverMetaId,
                          const std::vector<std::string>& fileNames,
                          std::set<std::string>& dataFileSet,
                          std::set<std::string>& uselessMetaFileSet, 
                          std::string& recoverMetaPath);
    static int32_t GetVersionId(const std::string& fileName);
    static std::string GetDescription(const std::string& fileName);
    static std::string GetPackageDataFileName(const std::string& description,
                                              uint32_t dataFileIdx);
    static std::string GetPackageMetaFileName(const std::string& description,
                                              int32_t metaVersionId);
    
private:
    int32_t mVersionId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionedPackageFileMeta);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_VERSIONED_PACKAGE_FILE_META_H
