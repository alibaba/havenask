#include <autil/StringUtil.h>
#include "indexlib/index_base/index_meta/index_path_util.h"
// #include "indexlib/misc/regular_expression.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_BEGIN(index_base);

bool IndexPathUtil::GetSegmentId(const std::string& path, segmentid_t& segmentId) noexcept
{
    vector<string> frags = StringUtil::split(path, "_", false);
    if (frags.size() == 4)
    {
        uint32_t levelId = 0;
        return frags[0] == SEGMENT_FILE_NAME_PREFIX
            && frags[2] == "level"
            && StringUtil::fromString<segmentid_t>(frags[1], segmentId)
            && StringUtil::fromString<uint32_t>(frags[3], levelId);
    }
    else if (frags.size() == 2)
    {
        return frags[0] == SEGMENT_FILE_NAME_PREFIX
            && StringUtil::fromString<segmentid_t>(frags[1], segmentId);
    }
    return false;
}

bool IndexPathUtil::GetVersionId(const std::string& path, versionid_t& versionId) noexcept
{
    string prefix = VERSION_FILE_NAME_PREFIX ".";
    return StringUtil::startsWith(path, prefix)
        && StringUtil::fromString<versionid_t>(path.substr(prefix.length()), versionId);
}

bool IndexPathUtil::GetDeployMetaId(const std::string& path, versionid_t& versionId) noexcept
{
    string prefix = DEPLOY_META_FILE_NAME_PREFIX ".";
    return StringUtil::startsWith(path, prefix)
        && StringUtil::fromString<versionid_t>(path.substr(prefix.length()), versionId);
}

bool IndexPathUtil::GetPatchMetaId(const std::string& path, versionid_t& versionId) noexcept
{
    string prefix = PARTITION_PATCH_META_FILE_PREFIX ".";
    return StringUtil::startsWith(path, prefix)
        && StringUtil::fromString<versionid_t>(path.substr(prefix.length()), versionId);
}

bool IndexPathUtil::GetSchemaId(const std::string& path, schemavid_t& schemaId) noexcept
{
    if (path == SCHEMA_FILE_NAME)
    {
        schemaId = DEFAULT_SCHEMAID;
        return true;
    }
    string prefix = SCHEMA_FILE_NAME + ".";
    return StringUtil::startsWith(path, prefix)
        && StringUtil::fromString<schemavid_t>(path.substr(prefix.length()), schemaId);
}

bool IndexPathUtil::GetPatchIndexId(const std::string& path, schemavid_t& schemaId) noexcept
{
    string prefix = PARTITION_PATCH_DIR_PREFIX "_";
    return StringUtil::startsWith(path, prefix)
        && StringUtil::fromString<schemavid_t>(path.substr(prefix.length()), schemaId);
}

IE_NAMESPACE_END(index_base);
