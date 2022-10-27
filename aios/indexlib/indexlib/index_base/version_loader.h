#ifndef __INDEXLIB_VERSION_LOADER_H
#define __INDEXLIB_VERSION_LOADER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

IE_NAMESPACE_BEGIN(index_base);

class VersionLoader
{
public:
    VersionLoader();
    ~VersionLoader();
public:
    static void GetVersion(const std::string& rootDir,
                           Version &version,
                           versionid_t versionId);
    static void GetVersion(const file_system::DirectoryPtr& directory,
                           Version &version, versionid_t versionId);
    static void GetVersion(const std::string& versionFilePath, 
                           Version& version);

    static void ListVersion(const std::string& rootDir,
                            fslib::FileList &fileList);
    static void ListVersion(const file_system::DirectoryPtr& directory,
                            fslib::FileList &fileList, bool localOnly = false);

    static void ListSegments(const std::string& rootDir,
                             fslib::FileList &fileList);
    static void ListSegments(const file_system::DirectoryPtr& directory,
                             fslib::FileList &fileList, bool localOnly = false);

    static versionid_t GetVersionId(const std::string& versionFileName);

    static bool IsValidVersionFileName(const std::string& fileName);
    
    static bool IsValidSegmentFileName(const std::string& fileName);

private:
    static int32_t extractSuffixNumber(const std::string& name,
            const std::string& prefix);

    static bool MatchPattern(const std::string& fileName, 
                             const std::string& prefix, 
                             char sep);

    static bool NeedSkipSecondaryRoot(const std::string& rootDir);

    friend struct VersionFileComp;
    friend struct SegmentDirComp;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionLoader);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_VERSION_LOADER_H
