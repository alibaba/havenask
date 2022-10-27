#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/StringUtil.h>
using namespace std;
using namespace autil::legacy;
using namespace autil;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, ParallelBuildInfo);

const std::string ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE = "parallel_build_info_file";
const std::string ParallelBuildInfo::PARALLEL_BUILD_PREFIX = "parallel_";
const std::string ParallelBuildInfo::PARALLEL_BUILD_INSTANCE_PREFIX = "instance_";
ParallelBuildInfo::ParallelBuildInfo()
    : parallelNum(0u)
    , batchId(0u)
    , instanceId(0u)
    , baseVersion(INVALID_VERSION)
{
}

ParallelBuildInfo::ParallelBuildInfo(uint32_t _parallelNum,
                                     uint32_t _batchId, uint32_t _instanceId)
    : parallelNum(_parallelNum)
    , batchId(_batchId)
    , instanceId(_instanceId)
    , baseVersion(INVALID_VERSION)
{}

ParallelBuildInfo::~ParallelBuildInfo()
{
}

void ParallelBuildInfo::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("parallel_num", parallelNum, parallelNum);
    json.Jsonize("instance_id", instanceId, instanceId);
    json.Jsonize("batch_id", batchId, batchId);
    json.Jsonize("base_version", baseVersion, baseVersion);
}

bool ParallelBuildInfo::IsParallelBuild() const
{
    //return !(1 == parallelNum && 0 == instanceId && parentDir.empty());
    return parallelNum || instanceId;
}

void ParallelBuildInfo::CheckValid(const std::string &rootPath)
{
    if (!IsParallelBuild())
    {
        return;
    }

    if (parallelNum < 1 || instanceId >= parallelNum)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "parallel build instanceid or parallelnum is incorrect. "
                             "parallelNum [%u], instance id[%u]", parallelNum, instanceId);
    }

    if (!FileSystemWrapper::IsExist(rootPath))
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "parallel build  dir not exist. "
                             "instance root path [%s]", rootPath.c_str());
    }

}

bool ParallelBuildInfo::IsExist(const std::string &dir)
{
    return FileSystemWrapper::IsExist(
            FileSystemWrapper::JoinPath(dir, PARALLEL_BUILD_INFO_FILE));
}

void ParallelBuildInfo::Load(const std::string& dir)
{
    string filePath = FileSystemWrapper::JoinPath(dir, PARALLEL_BUILD_INFO_FILE);
    if (IsExist(dir))
    {
        std::string content;
        FileSystemWrapper::AtomicLoad(filePath, content);
        FromJsonString(*this, content);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(NonExist, "file [%s] not exist!", filePath.c_str());
    }
}

std::string ParallelBuildInfo::GetParallelInstancePath(const string& rootPath) const
{
    return FileSystemWrapper::JoinPath(GetParallelPath(rootPath),
            PARALLEL_BUILD_INSTANCE_PREFIX + StringUtil::toString(instanceId));
}

std::string ParallelBuildInfo::GetParallelPath(const string& rootPath) const
{
    return FileSystemWrapper::JoinPath(rootPath,
            PARALLEL_BUILD_PREFIX + StringUtil::toString(parallelNum) + "_" +
            StringUtil::toString(batchId));
}

std::string ParallelBuildInfo::GetParallelPath(const string& rootPath,
        uint32_t parallelNum, uint32_t batchId)
{
    return ParallelBuildInfo(parallelNum, batchId, 0).GetParallelPath(rootPath);
}

bool ParallelBuildInfo::operator==(const ParallelBuildInfo& other) const
{
    return parallelNum == other.parallelNum
        && batchId == other.batchId
        && instanceId == other.instanceId
        && baseVersion == other.baseVersion;
}

void ParallelBuildInfo::StoreIfNotExist(const std::string& dir)
{
    if (!IsExist(dir))
    {
        FileSystemWrapper::AtomicStore(
                FileSystemWrapper::JoinPath(dir, PARALLEL_BUILD_INFO_FILE),
                ToJsonString(*this));
    }
    else
    {
        ParallelBuildInfo info;
        info.Load(dir);
        if (*this != info)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "parallelBuildInfo [%s] not equal to local file [%s]!",
                    ToJsonString(*this).c_str(),
                    ToJsonString(info).c_str());
        }
    }
}

bool ParallelBuildInfo::IsParallelBuildPath(const std::string& path)
{
    // start with paralll_
    if (0 != path.find(PARALLEL_BUILD_PREFIX))
    {
        return false;
    }
    std::string numberString = path.substr(PARALLEL_BUILD_PREFIX.length());
    vector<std::string> numbers = StringUtil::split(numberString, "_");
    if (2 != numbers.size())
    {
        return false;
    }
    uint32_t parallelNum;
    uint32_t batchId;
    return StringUtil::fromString(numbers[0], parallelNum) &&
        StringUtil::fromString(numbers[1], batchId);
}

IE_NAMESPACE_END(index_base);

