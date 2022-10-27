#include "indexlib/index_base/index_meta/segment_file_meta.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"

using namespace std;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentFileMeta);

SegmentFileMeta::SegmentFileMeta()
    : mHasPackageFile(false)
    , mIsSub(false)
{
}

SegmentFileMeta::~SegmentFileMeta() 
{
}

SegmentFileMetaPtr SegmentFileMeta::Create(const file_system::DirectoryPtr& segDir, bool isSub)
{
    SegmentFileMetaPtr meta(new SegmentFileMeta);
    if (!meta->Load(segDir, isSub))
    {
        meta.reset();
    }
    return meta;
}

bool SegmentFileMeta::Load(const file_system::DirectoryPtr& segDirectory, bool isSub)
{
    mRoot = segDirectory->GetPath();
    mIsSub = isSub;
    IndexFileList meta;
    if (!SegmentFileListWrapper::Load(segDirectory, meta))
    {
        return false;
    }
    if (isSub) {
        return LoadSub(segDirectory, meta);
    } else {
        return Load(segDirectory, meta);
    }
}

bool SegmentFileMeta::Load(const file_system::DirectoryPtr& segDirectory, IndexFileList& meta)
{
    for (auto & f : meta.deployFileMetas)
    {
        mFileMetas.insert(make_pair(f.filePath, fslib::FileMeta{f.fileLength, 0LL, 0LL}));
    }

    // index_file_list not in dp file lists, add itself to solid path
    IndexFileList::FileInfoVec targetFileInfos = meta.deployFileMetas;
    targetFileInfos.push_back(FileInfo(meta.GetFileName(), meta.ToString().size()));
    segDirectory->AddSolidPathFileInfos(targetFileInfos.begin(), targetFileInfos.end());
    if (!segDirectory->SetLifecycle(meta.lifecycle))
    {
        return false;
    }
    if (!LoadPackageFileMeta(segDirectory, false))
    {
        return false;
    }
    if (IsExist(SUB_SEGMENT_DIR_NAME))
    {
        return LoadPackageFileMeta(segDirectory, true);
    }
    return true;
}

bool SegmentFileMeta::LoadSub(const file_system::DirectoryPtr& segDirectory, IndexFileList& meta)
{
    if (!Load(segDirectory, meta))
    {
        return false;
    }
    auto subNameSize = strlen(SUB_SEGMENT_DIR_NAME);
    std::map<std::string, fslib::FileMeta> tmpFileMetas;
    for (auto iter = mFileMetas.begin(); iter != mFileMetas.end(); iter ++)
    {
        string filePath = iter->first;
        auto meta = iter->second;
        if (filePath.substr(0, subNameSize) != SUB_SEGMENT_DIR_NAME
            || filePath == string(SUB_SEGMENT_DIR_NAME) + '/')
        {
            continue;
        }
        tmpFileMetas.insert(make_pair(filePath.substr(subNameSize + 1), meta));
    }
    mFileMetas.swap(tmpFileMetas);
    return true;
}

bool SegmentFileMeta::LoadPackageFileMeta(const file_system::DirectoryPtr& segDirectory, bool appendSubName)
{
    string content;
    string packageFileName = PACKAGE_FILE_PREFIX;
    PackageFileMeta packageFileMeta;
    if (appendSubName)
    {
        packageFileName = util::PathUtil::JoinPath(SUB_SEGMENT_DIR_NAME, packageFileName);
    }
    string packageFileMetaName = PackageFileMeta::GetPackageFileMetaPath(packageFileName);
    if (!IsExist(packageFileMetaName))
    {
        return true;
    }
    mHasPackageFile = true;
    try
    {
        segDirectory->Load(packageFileMetaName, content, true);
        packageFileMeta.InitFromString(content);
    }
    catch (NonExistException& e)
    {
        IE_LOG(DEBUG, "%s", e.GetMessage().c_str());
        return false;
    }
    catch (ExceptionBase& e)
    {
        IE_LOG(WARN, "Load [%s/%s] failed, reason [%s]", segDirectory->GetPath().c_str(),
               packageFileMetaName.c_str(), e.GetMessage().c_str());
        return false;
    }
    catch (...)
    {
        IE_LOG(WARN, "Load [%s/%s] failed", segDirectory->GetPath().c_str(),
               packageFileMetaName.c_str());
        return false;
    }

    PackageFileMeta::Iterator iter = packageFileMeta.Begin();
    for ( ; iter != packageFileMeta.End(); ++iter)
    {
        const PackageFileMeta::InnerFileMeta& innerFileMeta = *iter;
        std::string filePath = innerFileMeta.GetFilePath();
        if (innerFileMeta.IsDir())
        {
            filePath += '/';
        }
        if (appendSubName)
        {
            filePath = util::PathUtil::JoinPath(SUB_SEGMENT_DIR_NAME, filePath);
        }
        fslib::FileMeta meta;
        meta.fileLength = innerFileMeta.GetLength();
        meta.createTime = -1;
        meta.lastModifyTime = -1;
        mFileMetas.insert(make_pair(filePath, meta));
    }
    mFileMetas.erase(packageFileMetaName);
    for (uint32_t i = 0; i < packageFileMeta.GetPhysicalFileNames().size(); ++i)
    {
        string packageFileDataPath = PackageFileMeta::GetPackageFileDataPath(
                packageFileName, packageFileMeta.GetPhysicalFileTag(i), i);
        mFileMetas.erase(packageFileDataPath);
    }

    return true;
}

void SegmentFileMeta::ListFile(const std::string& dirPath,
                               fslib::FileList& fileList,
                               bool recursive) const
{
    string normalizePath = util::PathUtil::NormalizePath(dirPath);
    auto iter = mFileMetas.upper_bound(normalizePath);
    for (; iter != mFileMetas.end(); ++iter)
    {
        const string& path = iter->first;
        if (path.size() < normalizePath.size()
            || path.compare(0, normalizePath.size(), normalizePath) != 0)
        {
            break;
        }

        string subDir;
        if (IsSubDirectory(normalizePath, path, recursive, subDir))
        {
            fileList.push_back(subDir);
        }
    }
}

void SegmentFileMeta::ListFile(const std::string& dirPath,
                               vector<pair<string, int64_t>>& fileList,
                               bool recursive) const
{
    string normalizePath = util::PathUtil::NormalizePath(dirPath);
    auto iter = mFileMetas.upper_bound(normalizePath);
    for (; iter != mFileMetas.end(); ++iter)
    {
        const string& path = iter->first;
        if (path.size() < normalizePath.size()
            || path.compare(0, normalizePath.size(), normalizePath) != 0)
        {
            break;
        }
        pair<string, int64_t> fileInfo;
        if (IsSubDirectory(normalizePath, path, recursive, fileInfo.first))
        {
            fileInfo.second = iter->second.fileLength < 0 ? 0 : iter->second.fileLength;
            fileList.push_back(fileInfo);
        }
    }
}

int64_t SegmentFileMeta::CalculateDirectorySize(const std::string& dirPath)
{
    vector<pair<string, int64_t>> fileList;
    ListFile(dirPath, fileList, true);
    int64_t indexSize = 0;
    for (auto &p : fileList)
    {
        indexSize += p.second;
    }
    return indexSize;
}

bool SegmentFileMeta::IsSubDirectory(const std::string& normalizePath, const std::string& path,
                                     bool recursive, std::string& subDir) const
{
    // ListFile("index"), skip "index/"
    if (normalizePath + '/' == path)
    {
        return false;
    }
        
    // ListFile("index"), skip "index/*/yy"
    auto pos = path.find('/', normalizePath.size() + 1);
    if (!recursive && pos != string::npos && pos + 1 != path.size())
    {
        return false;
    }

    size_t st = normalizePath.size() + 1;
    if (normalizePath.empty())
    {
        st = 0;
    }
    if (path.back() == '/' && !recursive)
    {
        subDir = path.substr(st, path.size() - st - 1);
    }
    else
    {
        subDir = path.substr(st);
    }
    return true;
}


bool SegmentFileMeta::IsExist(const std::string& filePath) const
{
    string normalizePath = util::PathUtil::NormalizePath(filePath);
    auto it = mFileMetas.lower_bound(normalizePath);
    if (it == mFileMetas.end())
    {
        return false;
    }
    const string& path = it->first;
    if (normalizePath == path || normalizePath + '/' == path)
    {
        return true;
    }
    return false;
}

size_t SegmentFileMeta::GetFileLength(const std::string& fileName, bool ignoreNoEntry) const
{
    string normalizePath = util::PathUtil::NormalizePath(fileName);
    auto it = mFileMetas.lower_bound(normalizePath);
    if (it == mFileMetas.end() || it->first != normalizePath)
    {
        if (!ignoreNoEntry)
        {
            IE_LOG(WARN, "segment directory: [%s], File [%s] not exist.",
                   mRoot.c_str(), fileName.c_str());
        }
        return 0;
    }
    return it->second.fileLength;
}

void SegmentFileMeta::TEST_Print()
{
    cout << "SegmentFileMeta " << mRoot << ", issub: " << mIsSub << endl;
    for (auto iter = mFileMetas.begin(); iter != mFileMetas.end(); ++iter)
    {
        cout << "File: " << iter->first << endl;
    }
}

IE_NAMESPACE_END(index_base);

