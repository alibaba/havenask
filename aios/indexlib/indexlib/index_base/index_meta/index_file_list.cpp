#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include <autil/StringUtil.h>
#include <algorithm>    // std::set_difference, std::sort, std::remove_if

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, IndexFileList);

void IndexFileList::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("deploy_file_metas", deployFileMetas, deployFileMetas);
    json.Jsonize("lifecycle", lifecycle, lifecycle);    
    if (json.GetMode() != TO_JSON || !finalDeployFileMetas.empty())
    {
        json.Jsonize("final_deploy_file_metas",
                     finalDeployFileMetas, finalDeployFileMetas);
    }
}

void IndexFileList::FromString(const string& indexMetaStr)
{
    deployFileMetas.clear();
    if (indexMetaStr.size() > 0 && indexMetaStr[0] == '{')
    {
        autil::legacy::FromJsonString(*this, indexMetaStr);
    }
    else
    {
        vector<string> fileList = StringUtil::split(indexMetaStr, "\n");
        for (size_t i = 0; i < fileList.size(); ++i)
        {
            deployFileMetas.push_back(FileInfo(fileList[i]));
        }
    }
}

string IndexFileList::ToString() const
{
    return autil::legacy::ToJsonString(*this);
} 

void IndexFileList::Append(const FileInfo& fileMeta, bool isFinal)
{
    if (isFinal)
    {
        AppendFinal(fileMeta);
    }
    else
    {
        deployFileMetas.push_back(fileMeta);
    }
}

void IndexFileList::AppendFinal(const FileInfo& fileMeta)
{
    finalDeployFileMetas.push_back(fileMeta);
}

bool IndexFileList::Load(const string& path, bool* exist)
{
    string content;
    if (exist)
    {
        *exist = true;
    }
    try
    {
        if (!storage::FileSystemWrapper::AtomicLoad(path, content, true))
        {
            IE_LOG(INFO, "Load [%s] failed, not exist!", path.c_str());
            if (exist)
            {
                *exist = false;
            }
            return false;
        }
        FromString(content);
    }
    catch (...)
    {
        IE_LOG(INFO, "Load [%s] failed", path.c_str());
        return false;
    }
    mFileName = util::PathUtil::GetFileName(path);
    return true;
}

bool IndexFileList::Load(const DirectoryPtr& directory, const string& fileName,
                         bool* exist)
{
    string content;
    if (exist)
    {
        *exist = true;
    }
    try
    {
        directory->Load(fileName, content, true);
        FromString(content);
    }
    catch (NonExistException& e)
    {
        IE_LOG(INFO, "Load [%s] failed, file is not exist", fileName.c_str());
        if (exist)
        {
            *exist = false;
        }
        return false;
    }
    catch (...)
    {
        IE_LOG(INFO, "Load [%s/%s] failed", directory->GetPath().c_str(), fileName.c_str());
        return false;
    }
    
    mFileName = fileName;
    return true;
}

void IndexFileList::Sort()
{
    sort(deployFileMetas.begin(), deployFileMetas.end());
    sort(finalDeployFileMetas.begin(), finalDeployFileMetas.end());
}

void IndexFileList::FromDifference(IndexFileList& lhs, IndexFileList& rhs)
{
    lhs.Sort();
    rhs.Sort();

    deployFileMetas.clear();
    set_difference(lhs.deployFileMetas.begin(), lhs.deployFileMetas.end(),
                   rhs.deployFileMetas.begin(), rhs.deployFileMetas.end(),
                   inserter(deployFileMetas, deployFileMetas.begin()));

    finalDeployFileMetas.clear();
    set_difference(lhs.finalDeployFileMetas.begin(), lhs.finalDeployFileMetas.end(),
                   rhs.finalDeployFileMetas.begin(), rhs.finalDeployFileMetas.end(),
                   inserter(finalDeployFileMetas, finalDeployFileMetas.begin()));
}

std::string IndexFileList::GetFileName() const
{
    return mFileName;
}

void IndexFileList::Filter(function<bool(const FileInfo&)> predicate)
{
    deployFileMetas.erase(remove_if(deployFileMetas.begin(), deployFileMetas.end(), predicate),
                          deployFileMetas.end());
    finalDeployFileMetas.erase(remove_if(finalDeployFileMetas.begin(), finalDeployFileMetas.end(), predicate),
                               finalDeployFileMetas.end());
}

IE_NAMESPACE_END(index_base);

