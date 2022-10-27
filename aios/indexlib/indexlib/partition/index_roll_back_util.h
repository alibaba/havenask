#ifndef __INDEXLIB_INDEX_ROLL_BACK_UTIL_H
#define __INDEXLIB_INDEX_ROLL_BACK_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(partition);

class IndexRollBackUtil
{
public:
    IndexRollBackUtil();
    ~IndexRollBackUtil();
public:
    static versionid_t GetLatestOnDiskVersion(const std::string& indexRoot);
    static bool CreateRollBackVersion(const std::string& rootDir,
                                      versionid_t sourceVersionId,
                                      versionid_t targetVersionId);
    static bool CheckVersion(const std::string& indexRoot, versionid_t versionId);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexRollBackUtil);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_ROLL_BACK_UTIL_H
