#ifndef __INDEXLIB_DEPLOY_FILES_WRAPPER_H
#define __INDEXLIB_DEPLOY_FILES_WRAPPER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/online_config.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/index_file_list.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, LoadConfig);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);

IE_NAMESPACE_BEGIN(index_base);

typedef IndexFileList DeployIndexMeta;

class DeployIndexWrapper
{
public:
    // newPath is remote path, lastPath is local path
    DeployIndexWrapper(const std::string& newPath, const std::string& lastPath = "",
                       const config::OnlineConfig& onlineConfig = config::OnlineConfig());
    ~DeployIndexWrapper();

public:
    // TODO: delete
    // TODO:
    // 1. delete obsolete files in old version
    // 2. do not deploy meta files
    bool GetDeployFiles(fslib::FileList& fileList,
                        const versionid_t newVersion,
                        const versionid_t lastVersion = INVALID_VERSION);
    // HINT: used in build_service::MergerServiceImpl
    bool GetIndexSize(int64_t &totalLength,
                      const versionid_t newVersion,
                      const versionid_t lastVersion = INVALID_VERSION);

    bool DumpSegmentDeployIndex(const std::string& segDirName, const std::string& lifecycle = "");
    bool DumpTruncateMetaIndex();
    bool DumpAdaptiveBitmapMetaIndex();

    // TODO: deprecated. but for now suez still in use. Please use [static bool GetDeployIndexMeta] instead
    bool GetDeployIndexMeta(DeployIndexMeta& deployIndexMeta,
                            const versionid_t newVersion,
                            const versionid_t lastVersion = INVALID_VERSION,
                            bool needComplete = true);

    static bool GetDeployIndexMeta(
        DeployIndexMeta& remoteDeployIndexMeta,  // files will read from remote, for now maybe dcache or mpangu
        DeployIndexMeta& localDeployIndexMeta,   // files will read from local
        const std::string& sourcePath, // offline path, target version file must be in sourcePath
        const std::string& localPath,  // base version file must be in localPath
        versionid_t targetVersionId,
        versionid_t baseVersionId,     // may be INALID_VERSION_ID
        const config::OnlineConfig& targetOnlineConfig,
        const config::OnlineConfig& baseOnlineConfig) noexcept;

public:
    static bool DumpSegmentDeployIndex(const std::string& segmentPath,
        const fslib::FileList& fileList, const std::string& lifecycle = "");

    static void DumpSegmentDeployIndex(const file_system::DirectoryPtr& directory,
        const SegmentInfoPtr& segmentInfo = SegmentInfoPtr(), const std::string& lifecycle = "");

    static bool GetSegmentSize(const index_base::SegmentData& segmentData,
                               bool includePatch, int64_t& totalLength);

    template<typename PathType>
    static bool GetSegmentSize(const PathType& directory,
                               bool includePatch, int64_t& totalLength) {
        IndexFileList meta;
        if (!SegmentFileListWrapper::Load(directory, meta))
        {
            return false;
        }
        return DoGetSegmentSize(meta, includePatch, totalLength);
    }
    
    // for final deploy index meta bind with version, throw exception when failed
    static void DumpDeployMeta(const std::string& partitionPath, const Version& version);
    static void DumpDeployMeta(const file_system::DirectoryPtr& partitionDirectory,
                               const Version& version);

    // TODO: replace this interface with DumpDeployMeta
    template<typename PathType>
    static void DumpDeployIndexMeta(const PathType& partitionPath, const Version& version) {
        DumpDeployMeta(partitionPath, version);
    }
    static std::string GetDeployMetaFileName(versionid_t versionId);
    static std::string GetDeployMetaLockFileName(versionid_t versionId);

private:
    bool GetDiffSegmentIds(std::vector<segmentid_t>& segIds,
                           const Version& newVersion,
                           const versionid_t newVersionId,
                           const versionid_t lastVersionId);
    bool GetTruncateMetaDeployFiles(fslib::FileList& fileList, int64_t &totalLength);
    bool GetAdaptiveBitmapMetaDeployFiles(fslib::FileList& fileList, int64_t &totalLength);
    bool GetSegmentDeployFiles(const Version& version,
                               const std::vector<segmentid_t>& segIds,
                               fslib::FileList& fileList, int64_t &totalLength);
    
    bool GetPatchSegmentDeployFiles(const Version& newVersion,
                                    const versionid_t lastVersionId,
                                    const std::vector<segmentid_t>& segIds,
                                    fslib::FileList& fileList, int64_t &totalLength);

    bool GetFiles(const std::string &relativePath,
                  fslib::FileList &fileList, int64_t &totalLength);

    bool DoGetDeployFiles(fslib::FileList& fileList, int64_t &totalLength,
                          const versionid_t newVersionId,
                          const versionid_t lastVersionId);
    bool NeedDeploy(const std::string& filePath, const std::string& lifecycle);

    void MergeIndexFileList(IndexFileList& finalIndexFileList,
                              const std::string& dirName);
    void FillBaseIndexFileList(IndexFileList& finalIndexFileList);
    void FillSegmentIndexFileList(const Version& version,
                                    IndexFileList& finalIndexFileList);
    bool DoGetDeployIndexMeta(IndexFileList& deployIndexMeta,
                              const versionid_t newVersion,
                              const versionid_t lastVersion,
                              bool needComplete);

private:
    static bool DoGetDeployIndexMeta(
        DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
        const std::string& sourcePath, const std::string& localPath,
        versionid_t targetVersionId, versionid_t baseVersionId,
        const config::OnlineConfig& targetOnlineConfig,
        const config::OnlineConfig& baseOnlineConfig);

    static bool DoGetSegmentSize(const IndexFileList& meta,
                                 bool includePatch, int64_t& totalLength);
    static void FillIndexFileList(const file_system::DirectoryPtr& partitionDirectory,
            const Version& version, IndexFileList& finalIndexFileList);
    static void FillBaseIndexFileList(const file_system::DirectoryPtr& partitionDirectory,
            const Version& version, IndexFileList& finalIndexFileList);
    static void FillOneIndexFileList(const file_system::DirectoryPtr& partitionDirectory,
            const std::string& fileName, IndexFileList& finalIndexFileList);
    static void MergeIndexFileList(const file_system::DirectoryPtr& partitionDirectory,
            const std::string& dirName, IndexFileList& finalIndexFileList);
    static void FillSegmentIndexFileList(const file_system::DirectoryPtr& partitionDirectory,
            const Version& version, IndexFileList& finalIndexFileList);
    static void CompleteIndexFileList(const std::string& rootPath, IndexFileList& deployIndexMeta);
    static const file_system::LoadConfig& GetLoadConfig(
        const config::OnlineConfig& onlineConfig,
        const std::string& filePath, const std::string& lifecycle);
    static void PickFiles(
        DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
        const IndexFileList& targetIndexFileList, const IndexFileList& baseIndexFileList,
        const config::OnlineConfig& targetOnlineConfig, const config::OnlineConfig& baseOnlineConfig,
        const file_system::LoadConfig& targetFilterConfig, const file_system::LoadConfig& baseFilterConfig);
    static bool GenerateLoadConfigFromVersion(const std::string& indexRoot,
                                              versionid_t versionId,
                                              file_system::LoadConfig& loadConfig);
private:
    std::string mNewPath;
    std::string mLastPath;
    config::OnlineConfig mOnlineConfig;
    friend class DeployIndexWrapperTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeployIndexWrapper);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_DEPLOY_FILES_WRAPPER_H
