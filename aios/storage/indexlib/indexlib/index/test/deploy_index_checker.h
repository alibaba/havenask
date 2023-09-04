#ifndef __INDEXLIB_DEPLOY_INDEX_CHECKER_H
#define __INDEXLIB_DEPLOY_INDEX_CHECKER_H

#include <memory>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DeployIndexChecker
{
public:
    static void CheckDeployIndexMeta(const std::string& dir, versionid_t newVersion, versionid_t lastVersion,
                                     fslib::FileList expectMore = fslib::FileList(),
                                     fslib::FileList expectLess = fslib::FileList());

private:
    static fslib::FileList DeployIndexMeta2FileList(const file_system::IndexFileList& deployIndexMeta);
};

///////////////////////////////////////////////////////////////////////////
void DeployIndexChecker::CheckDeployIndexMeta(const std::string& dir, versionid_t newVersion, versionid_t lastVersion,
                                              fslib::FileList expectMore, fslib::FileList expectLess)
{
    index_base::DeployIndexWrapper::GetDeployIndexMetaInputParams params;
    params.rawPath = dir;
    params.targetVersionId = newVersion;
    params.baseVersionId = lastVersion;
    file_system::DeployIndexMetaVec localDeployIndexMetaVec;
    file_system::DeployIndexMetaVec remoteDeployIndexMetaVec;
    ASSERT_TRUE(
        index_base::DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec));
    const file_system::IndexFileList& deployIndexMeta = *localDeployIndexMetaVec[0];

    // TODO: ZQ, NEED new check
    // version.NEWVERSION in finalDeployFileMetas
    ASSERT_EQ(1, deployIndexMeta.finalDeployFileMetas.size());

    // check by old API FileList
    // fslib::FileList expectFileList;
    // deployIndexWrapper.GetDeployFiles(expectFileList, newVersion, lastVersion);
    // fslib::FileList actualFileList = DeployIndexMeta2FileList(deployIndexMeta);

    // +1 for deploy_meta.NEWVERSION, +N for segment_*
    // EXPECT_LE(expectFileList.size(), actualFileList.size() + expectLess.size());

    // check file list
    // sort(expectFileList.begin(), expectFileList.end());
    // sort(actualFileList.begin(), actualFileList.end());

    // fslib::FileList more;
    // set_difference(actualFileList.begin(), actualFileList.end(),
    //                expectFileList.begin(), expectFileList.end(),
    //                inserter(more, more.begin()));
    // bool success = true;

    // for (auto file : more)
    // {
    //     bool isDeployMetaFile = autil::StringUtil::startsWith(file, DEPLOY_META_FILE_NAME_PREFIX);
    //     bool isSegmentDir = autil::StringUtil::startsWith(file, SEGMENT_FILE_NAME_PREFIX)
    //                         && file.find("/") + 1 == file.size();
    //     if (!isDeployMetaFile && !isSegmentDir &&
    //         find(expectMore.begin(), expectMore.end(), file) == expectMore.end())
    //     {
    //         if (!autil::EnvUtil::getEnv("INDEXLIB_COMPATIBLE_DEPLOY_INDEX_FILE", true)
    //             || !autil::StringUtil::endsWith(file, "/deploy_index"))
    //         {
    //             success = false;
    //         }
    //         std::cout << "[More] " << file << std::endl;
    //     }
    // }
    // EXPECT_TRUE(success) << newVersion << ":" << lastVersion;

    // fslib::FileList less;
    // set_difference(expectFileList.begin(), expectFileList.end(),
    //                actualFileList.begin(), actualFileList.end(),
    //                inserter(less, less.begin()));
    // EXPECT_EQ(expectLess.size(), less.size());
    // success = true;
    // for (auto file : less)
    // {
    //     if (find(expectLess.begin(), expectLess.end(), file) == expectLess.end())
    //     {
    //         success = false;
    //         std::cout << "[Less] " << file << std::endl;
    //     }
    // }
    // EXPECT_TRUE(success) << newVersion << ":" << lastVersion;
}

fslib::FileList DeployIndexChecker::DeployIndexMeta2FileList(const file_system::IndexFileList& deployIndexMeta)
{
    std::vector<std::string> actualFileList;
    for (const auto& meta : deployIndexMeta.deployFileMetas) {
        if (meta.fileLength < 0) {
            if (*(meta.filePath.rbegin()) != '/' &&
                meta.filePath.find(file_system::PACKAGE_FILE_PREFIX) == std::string::npos &&
                !autil::StringUtil::endsWith(meta.filePath, SEGMENT_INFO_FILE_NAME) // for main segment_info
                && !autil::StringUtil::startsWith(meta.filePath, DEPLOY_META_FILE_NAME_PREFIX)) {
                std::cout << "[Bad] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
            } else {
                // std::cout << "[Omit] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime <<
                // std::endl;
            }
        } else if (meta.modifyTime == (uint64_t)-1) {
            if (!autil::StringUtil::endsWith(meta.filePath, COUNTER_FILE_NAME) &&
                !autil::StringUtil::endsWith(meta.filePath, SEGMENT_INFO_FILE_NAME)) // for sub segment_info
            {
                std::cout << "[Bad] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
            } else {
                // std::cout << "[Omit] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime <<
                // std::endl;
            }
        } else {
            // std::cout << "[Good] " << meta.filePath << ":" << meta.fileLength << ":" << meta.modifyTime << std::endl;
        }
        actualFileList.push_back(meta.filePath);
    }
    return actualFileList;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_DEPLOY_INDEX_CHECKER_H
