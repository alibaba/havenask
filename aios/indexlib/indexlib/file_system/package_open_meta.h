#ifndef __INDEXLIB_PACKAGE_OPEN_META_H
#define __INDEXLIB_PACKAGE_OPEN_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/common.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/file_carrier.h"

DECLARE_REFERENCE_CLASS(file_system, FileCarrier);

IE_NAMESPACE_BEGIN(file_system);

class PackageOpenMeta
{
public:
    PackageOpenMeta() = default;
    ~PackageOpenMeta() = default;

public:
    size_t GetOffset() const { return innerFileMeta.GetOffset(); }
    size_t GetLength() const { return innerFileMeta.GetLength(); }
    const std::string& GetLogicalFilePath() const { return logicalFilePath; }
    const std::string& GetPhysicalFilePath() const { return innerFileMeta.GetFilePath(); }
    void SetLogicalFilePath(const std::string& filePath)
    {
        logicalFilePath = filePath;
    }
    void SetPhysicalFilePath(const std::string& filePath)
    {
        innerFileMeta.SetFilePath(filePath);
    }
    ssize_t GetPhysicalFileLength() const { return physicalFileLength; }

    // for test
    bool operator ==(const PackageOpenMeta& other) const
    {
        return logicalFilePath == other.logicalFilePath &&
            innerFileMeta == other.innerFileMeta;
    }

public:
    PackageFileMeta::InnerFileMeta innerFileMeta;
    std::string logicalFilePath;
    FileCarrierPtr physicalFileCarrier;  // for dcache package optimize
    ssize_t physicalFileLength = -1;
};

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_OPEN_META_H
