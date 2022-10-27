#include "build_service/custom_merger/CustomMergerImpl.h"
#include "build_service/custom_merger/MergeResourceProvider.h"
#include "build_service/util/FileUtil.h"
#include <indexlib/config/schema_differ.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/storage/archive_folder.h>

using namespace std;
using namespace build_service::util;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(storage);

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergerImpl);

const std::string CustomMergerImpl::MERGE_INSTANCE_DIR_PREFIX = "instance_";

CustomMergerImpl::CustomMergerImpl() {
}

CustomMergerImpl::~CustomMergerImpl() {
}

bool CustomMergerImpl::init(CustomMergerInitParam& param) {
    _param = param;
    return true;
}

bool CustomMergerImpl::merge(const CustomMergeMeta& mergeMeta,
                                    size_t instanceId,
                                    const std::string& indexPath)
{
    CustomMergePlan plan;
    if (!mergeMeta.getMergePlan(instanceId, plan)) {
        BS_LOG(INFO, "no merge plan for instance id : %ld.", instanceId);
        return true;
    }
    string mergeDir = prepareInstanceDir(instanceId, indexPath);
    string checkpointPath = FileSystemWrapper::JoinPath(
            mergeDir, "checkpoint_dir");
    FileSystemWrapper::MkDirIfNotExist(checkpointPath);
    _checkpointFolder.reset(new ArchiveFolder(false));
    _checkpointFolder->Open(checkpointPath);
    vector<CustomMergePlan::TaskItem> tasks;
    plan.getTaskItems(tasks);
    for (auto &item: tasks) {
        if (hasCheckpoint(item, _checkpointFolder)) {
            continue;
        }
        if (!doMergeTask(item, mergeDir)) {
            return false;
        }
        if (!commitCheckpoint(item, _checkpointFolder)) {
            return false;
        }
    }
    _checkpointFolder->Close();
    return true;
}

bool CustomMergerImpl::hasCheckpoint(
        const CustomMergePlan::TaskItem& taskItem,
        const IE_NAMESPACE(storage)::ArchiveFolderPtr& checkpointFolder) {
    if (taskItem.taskIdx == -1) {
        return false;
    }
    FileWrapperPtr fileWrapper = checkpointFolder->GetInnerFile(
            taskItem.getTaskCheckpointName(), fslib::READ);
    return fileWrapper != FileWrapperPtr();
}

bool CustomMergerImpl::commitCheckpoint(
        const CustomMergePlan::TaskItem& taskItem, 
        const IE_NAMESPACE(storage)::ArchiveFolderPtr& checkpointFolder) {
    if (taskItem.taskIdx == -1) {
        return true;//do nothing
    }
    FileWrapperPtr fileWrapper = checkpointFolder->GetInnerFile(
            taskItem.getTaskCheckpointName(), fslib::WRITE);
    fileWrapper->Close();
    return true;
}

bool CustomMergerImpl::endMerge(const CustomMergeMeta& mergeMeta,
                                       const std::string& path,
                                       int32_t targetVersionId) {
    MergeResourceProviderPtr provider = _param.resourceProvider;
    PartitionResourceProviderPtr partitionProvider =
        provider->getPartitionResourceProvider();

    IndexPartitionSchemaPtr newSchema = provider->getNewSchema();
    string targetDir = FileSystemWrapper::JoinPath(
            path, partitionProvider->GetPatchRootDirName(newSchema));
    if (!FileSystemWrapper::IsExist(targetDir)) {
        FileSystemWrapper::MkDir(targetDir, true);
    }

    std::vector<std::string> instanceDirs = listInstanceDirs(path);
    vector<TaskItemDispatcher::SegmentInfo> segmentInfos;
    provider->getIndexSegmentInfos(segmentInfos);
    Version targetVersion = partitionProvider->GetVersion();

    for (auto& info: segmentInfos) {
        segmentid_t targetSegmentId = info.segmentId;
        string segDirName = targetVersion.GetSegmentDirName(targetSegmentId);
        string mergeDir = FileSystemWrapper::JoinPath(targetDir, segDirName);
        for (auto& instanceDir: instanceDirs) {
            auto segmentFiles = listInstanceFiles(instanceDir, segDirName);
            moveFiles(segmentFiles, mergeDir);
        }
    }
    if (targetVersionId != INVALID_VERSION) {
        partitionProvider->StoreVersion(newSchema, targetVersionId);
    } else {
        //for ops alter field not create version, but generate segment list
        partitionProvider->DumpDeployIndexForPatchSegments(provider->getNewSchema());
    }

    for (auto& instanceDir: instanceDirs) {
        FileSystemWrapper::DeleteDir(instanceDir);
    }
    return true;
}

std::vector<std::string> CustomMergerImpl::listInstanceFiles(
        const std::string& instanceDir,
        const string& subDir)
{
    std::vector<std::string> fileList;
    string dirPath = FileSystemWrapper::JoinPath(instanceDir, subDir);
    if (!FileSystemWrapper::IsExist(dirPath)) {
        return fileList;
    }
    FileSystemWrapper::ListDir(dirPath, fileList);

    for (size_t i = 0; i < fileList.size(); i++) {
        fileList[i] = FileSystemWrapper::JoinPath(dirPath, fileList[i]);
    }
    return fileList;
}

void CustomMergerImpl::moveFiles(const std::vector<std::string> &files,
                                        const std::string &targetDir)
{
    for (size_t i = 0; i < files.size(); i++) {
        string folder, fileName;
        FileUtil::splitFileName(files[i], folder, fileName);
        assert(fileName != SUB_SEGMENT_DIR_NAME);
        if (FileSystemWrapper::IsDir(files[i])) {
            string dest = FileSystemWrapper::JoinPath(targetDir, fileName);
            if (!FileSystemWrapper::IsExist(dest)) {
                FileSystemWrapper::MkDir(dest, true);
            }

            vector<string> subFiles;
            FileSystemWrapper::ListDir(files[i], subFiles);
            for (size_t j = 0; j < subFiles.size(); j++) {
                FileSystemWrapper::Rename(
                        FileSystemWrapper::JoinPath(files[i], subFiles[j]),
                        FileSystemWrapper::JoinPath(dest, subFiles[j]));
            }
        } else {
            FileSystemWrapper::Rename(files[i], FileSystemWrapper::JoinPath(targetDir, fileName));
        }
    }
}

uint32_t CustomMergerImpl::getOperationId() {
   auto newSchema = _param.resourceProvider->getNewSchema();
   if (newSchema->HasModifyOperations()) {
       return newSchema->GetSchemaVersionId();
   }
   return 0;
}

string CustomMergerImpl::getInstanceDirPrefix() {
    uint32_t operationId = getOperationId();
    string instanceDirPrefix;
    if (operationId == 0) {
        instanceDirPrefix = MERGE_INSTANCE_DIR_PREFIX;
    } else {
        instanceDirPrefix = autil::StringUtil::toString(operationId) + "_"
                          + MERGE_INSTANCE_DIR_PREFIX;                          
    }
    return instanceDirPrefix;
}

std::string CustomMergerImpl::prepareInstanceDir(
        size_t instanceId, const std::string& indexRoot) {
    string instanceDir =
        FileSystemWrapper::JoinPath(indexRoot,
                getInstanceDirPrefix() + "_" +
                autil::StringUtil::toString(instanceId));
    if (!FileSystemWrapper::IsExist(instanceDir))
    {
        //FileSystemWrapper::DeleteDir(instanceDir);
        FileSystemWrapper::MkDir(instanceDir, true);
    }
    return instanceDir;
}

std::vector<std::string> CustomMergerImpl::listInstanceDirs(const std::string & rootPath) {
    std::vector<std::string> fileList;
    FileSystemWrapper::ListDir(rootPath, fileList);

    std::vector<std::string> instanceDirList;
    string instanceDirPrefix = getInstanceDirPrefix();
    for (size_t i = 0; i < fileList.size(); i++)
    {
        string path = FileSystemWrapper::JoinPath(rootPath, fileList[i]);
        if (FileSystemWrapper::IsDir(path) &&
            0 == fileList[i].find(instanceDirPrefix))
        {
            instanceDirList.push_back(path);
        }
    }
    return instanceDirList;
}

map<string, AttributeConfigPtr> CustomMergerImpl::getNewAttributes() const {
    IndexPartitionSchemaPtr newSchema = _param.resourceProvider->getNewSchema();
    IndexPartitionSchemaPtr oldSchema = _param.resourceProvider->getOldSchema();
    assert(newSchema.get() != nullptr);
    assert(oldSchema.get() != nullptr);
    vector<AttributeConfigPtr> newAttrs;
    vector<IndexConfigPtr> newIndexes;
    string errorMsg;
    SchemaDiffer::CalculateAlterFields(oldSchema, newSchema,
                                       newAttrs, newIndexes, errorMsg);
    map<string, AttributeConfigPtr> newAttrMap;
    for (const auto &it : newAttrs) {
        newAttrMap[it->GetAttrName()] = it;
    }
    return newAttrMap;
}

MergeResourceProviderPtr CustomMergerImpl::getResourceProvider() const {
    return _param.resourceProvider;
}

}
}
