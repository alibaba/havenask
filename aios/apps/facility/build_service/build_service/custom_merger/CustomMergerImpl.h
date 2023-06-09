/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_BS_CUSTOMMERGERIMPL_H
#define ISEARCH_BS_CUSTOMMERGERIMPL_H

#include "build_service/common_define.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/CustomMergerTaskItem.h"
#include "build_service/util/Log.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"

namespace build_service { namespace custom_merger {

class CustomMergerImpl : public CustomMerger
{
public:
    CustomMergerImpl(uint32_t backupId = 0, const std::string& epochId = "");
    ~CustomMergerImpl();

private:
    CustomMergerImpl(const CustomMergerImpl&);
    CustomMergerImpl& operator=(const CustomMergerImpl&);

public:
    bool init(CustomMergerInitParam& param) override;
    bool merge(const CustomMergeMeta& mergeMeta, size_t instanceId, const std::string& path) override;
    bool endMerge(const CustomMergeMeta& mergeMeta, const std::string& path, int32_t targetVersionId) override;
    MergeResourceProviderPtr getResourceProvider() const override;

protected:
    virtual bool doMergeTask(const CustomMergePlan::TaskItem& taskItem,
                             const indexlib::file_system::DirectoryPtr& dir) = 0;
    virtual bool commitCheckpoint(const CustomMergePlan::TaskItem& taskItem,
                                  const indexlib::file_system::ArchiveFolderPtr& checkpointFolder);
    virtual bool hasCheckpoint(const CustomMergePlan::TaskItem& taskItem,
                               const indexlib::file_system::ArchiveFolderPtr& checkpointFolder);

    virtual std::string prepareInstanceDir(size_t instanceId, const std::string& indexRoot);
    virtual std::vector<std::string> listInstanceDirs(const std::string& rootPath);
    void mergeSegmentDir(const indexlib::file_system::DirectoryPtr& rootDirectory,
                         const indexlib::file_system::DirectoryPtr& segmentDirectory, const std::string& segmentPath,
                         indexlib::file_system::FenceContext* fenceContext);
    // virtual std::vector<std::string> listInstanceFiles(const std::string& instanceDir,
    // const std::string& subDir);
    // virtual void moveFiles(const std::vector<std::string> &files, const std::string &targetDir);

protected:
    // helper functions
    std::map<std::string, indexlib::config::AttributeConfigPtr> getNewAttributes() const;
    uint32_t getOperationId();
    std::string getInstanceDirPrefix();

public:
    static const std::string MERGE_INSTANCE_DIR_PREFIX;

protected:
    CustomMergerInitParam _param;
    indexlib::file_system::ArchiveFolderPtr _checkpointFolder;
    indexlib::file_system::FileSystemOptions _fileSystemOptions;
    uint32_t _backupId;
    std::string _epochId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerImpl);

}} // namespace build_service::custom_merger

#endif // ISEARCH_BS_CUSTOMMERGERIMPL_H
