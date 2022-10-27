#ifndef __INDEXLIB_DEPLOY_INDEX_CHECKER_H
#define __INDEXLIB_DEPLOY_INDEX_CHECKER_H

#include <tr1/memory>
#include <vector>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/util/env_util.h"

IE_NAMESPACE_BEGIN(index);

class DeployIndexChecker
{
public:
    static void CheckDeployIndexMeta(const std::string& dir,
            versionid_t newVersion, versionid_t lastVersion,
            fslib::FileList expectMore = fslib::FileList(),
            fslib::FileList expectLess = fslib::FileList());

private:
    static fslib::FileList DeployIndexMeta2FileList(
            const index_base::IndexFileList& deployIndexMeta);
};

///////////////////////////////////////////////////////////////////////////
void DeployIndexChecker::CheckDeployIndexMeta(const std::string& dir,
        versionid_t newVersion, versionid_t lastVersion,
        fslib::FileList expectMore, fslib::FileList expectLess)
{
    bool oldIndex = 
        !storage::FileSystemWrapper::IsExist(
            storage::FileSystemWrapper::JoinPath(
                dir, index_base::DeployIndexWrapper::GetDeployMetaFileName(newVersion)));

    index_base::DeployIndexWrapper deployIndexWrapper(dir);
    index_base::IndexFileList deployIndexMeta;
    deployIndexWrapper.GetDeployIndexMeta(deployIndexMeta, newVersion, lastVersion);

    // version.NEWVERSION in finalDeployFileMetas
    ASSERT_EQ(1, deployIndexMeta.finalDeployFileMetas.size());
    EXPECT_EQ(index_base::Version::GetVersionFileName(newVersion),
              deployIndexMeta.finalDeployFileMetas[0].filePath);
    fslib::FileMeta fileMeta;
    storage::FileSystemWrapper::GetFileMeta(
        storage::FileSystemWrapper::JoinPath(
            dir, index_base::Version::GetVersionFileName(newVersion)), fileMeta);
    EXPECT_EQ(oldIndex ? -1 : fileMeta.fileLength,
              deployIndexMeta.finalDeployFileMetas[0].fileLength);

    // check by old API FileList
    fslib::FileList expectFileList;
    deployIndexWrapper.GetDeployFiles(expectFileList, newVersion, lastVersion);
    fslib::FileList actualFileList = DeployIndexMeta2FileList(deployIndexMeta);

    
    // +1 for deploy_meta.NEWVERSION, +N for segment_*
    EXPECT_LE(expectFileList.size(), actualFileList.size() + expectLess.size());

    // check file list
    sort(expectFileList.begin(), expectFileList.end());
    sort(actualFileList.begin(), actualFileList.end());

    fslib::FileList more;
    set_difference(actualFileList.begin(), actualFileList.end(),
                   expectFileList.begin(), expectFileList.end(),
                   inserter(more, more.begin()));
    bool success = true;
    
    for (auto file : more)
    {
        bool isDeployMetaFile = autil::StringUtil::startsWith(file, DEPLOY_META_FILE_NAME_PREFIX);
        bool isSegmentDir = autil::StringUtil::startsWith(file, SEGMENT_FILE_NAME_PREFIX)
                            && file.find("/") + 1 == file.size();
        if (!isDeployMetaFile && !isSegmentDir &&
            find(expectMore.begin(), expectMore.end(), file) == expectMore.end())
        {
            if (!util::EnvUtil::GetEnv("INDEXLIB_COMPATIBLE_DEPLOY_INDEX_FILE", true)
                || !autil::StringUtil::endsWith(file, "/deploy_index"))
            {
                success = false;
            }
            std::cout << "[More] " << file << std::endl;
        }
    }
    EXPECT_TRUE(success) << newVersion << ":" << lastVersion;

    fslib::FileList less;
    set_difference(expectFileList.begin(), expectFileList.end(),
                   actualFileList.begin(), actualFileList.end(),
                   inserter(less, less.begin()));
    EXPECT_EQ(expectLess.size(), less.size());
    success = true;
    for (auto file : less)
    {
        if (find(expectLess.begin(), expectLess.end(), file) == expectLess.end())
        {
            success = false;
            std::cout << "[Less] " << file << std::endl;
        }
    }
    EXPECT_TRUE(success) << newVersion << ":" << lastVersion;
}


fslib::FileList DeployIndexChecker::DeployIndexMeta2FileList(
        const index_base::IndexFileList& deployIndexMeta)
{
    std::vector<std::string> actualFileList;
    for (const auto& meta : deployIndexMeta.deployFileMetas)
    {
        if (meta.fileLength < 0)
        {
            if (*(meta.filePath.rbegin()) != '/'
                && meta.filePath.find(PACKAGE_FILE_PREFIX) == std::string::npos
                && !autil::StringUtil::endsWith(meta.filePath, SEGMENT_INFO_FILE_NAME) // for main segment_info
                && !autil::StringUtil::startsWith(meta.filePath, DEPLOY_META_FILE_NAME_PREFIX))
            {
                std::cout << "[Bad] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
            }
            else
            {
                // std::cout << "[Omit] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
            }
        }
        else if (meta.modifyTime == (uint64_t)-1)
        {
            if (!autil::StringUtil::endsWith(meta.filePath, COUNTER_FILE_NAME)
                && !autil::StringUtil::endsWith(meta.filePath, SEGMENT_INFO_FILE_NAME)) // for sub segment_info
            {
                std::cout << "[Bad] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
            }
            else
            {
                // std::cout << "[Omit] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
            }
        }
        else
        {
            // std::cout << "[Good] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
        }
        actualFileList.push_back(meta.filePath);
    }
    return actualFileList;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEPLOY_INDEX_CHECKER_H
