#ifndef __INDEXLIB_INDEX_PATH_UTIL_H
#define __INDEXLIB_INDEX_PATH_UTIL_H

#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(index_base);

class IndexPathUtil
{
public:
    static bool GetSegmentId(const std::string& path, segmentid_t& segmentId) noexcept;
    static bool GetVersionId(const std::string& path, versionid_t& versionId) noexcept;
    static bool GetDeployMetaId(const std::string& path, versionid_t& versionId) noexcept;
    static bool GetPatchMetaId(const std::string& path, versionid_t& versionId) noexcept;
    static bool GetSchemaId(const std::string& path, schemavid_t& schemaId) noexcept;
    static bool GetPatchIndexId(const std::string& path, schemavid_t& schemaId) noexcept;
};

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INDEX_PATH_UTIL_H
