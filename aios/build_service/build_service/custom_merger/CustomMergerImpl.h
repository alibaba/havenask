#ifndef ISEARCH_BS_CUSTOMMERGERIMPL_H
#define ISEARCH_BS_CUSTOMMERGERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/CustomMergerTaskItem.h"
#include <indexlib/storage/archive_folder.h>

namespace build_service {
namespace custom_merger {

class CustomMergerImpl : public CustomMerger
{
public:
    CustomMergerImpl();
    ~CustomMergerImpl();
private:
    CustomMergerImpl(const CustomMergerImpl &);
    CustomMergerImpl& operator=(const CustomMergerImpl &);

public:
    bool init(CustomMergerInitParam& param) override;
    bool merge(const CustomMergeMeta& mergeMeta,
               size_t instanceId,
               const std::string& path) override;
    bool endMerge(const CustomMergeMeta& mergeMeta,
                  const std::string& path, int32_t targetVersionId) override;

protected:
    virtual bool doMergeTask(const CustomMergePlan::TaskItem& taskItem, const std::string& dir) = 0;
    virtual bool commitCheckpoint(const CustomMergePlan::TaskItem& taskItem, 
                                  const IE_NAMESPACE(storage)::ArchiveFolderPtr& checkpointFolder);
    virtual bool hasCheckpoint(const CustomMergePlan::TaskItem& taskItem,
                               const IE_NAMESPACE(storage)::ArchiveFolderPtr& checkpointFolder);
    
    virtual std::string prepareInstanceDir(size_t instanceId, const std::string& indexRoot);
    virtual std::vector<std::string> listInstanceDirs(const std::string & rootPath);
    virtual std::vector<std::string> listInstanceFiles(const std::string& instanceDir,
                                                       const std::string& subDir);
    virtual void moveFiles(const std::vector<std::string> &files, const std::string &targetDir);

    virtual MergeResourceProviderPtr getResourceProvider() const;
protected:
    // helper functions
    std::map<std::string, IE_NAMESPACE(config)::AttributeConfigPtr> getNewAttributes() const;
    uint32_t getOperationId();
    std::string getInstanceDirPrefix();
public:
    static const std::string MERGE_INSTANCE_DIR_PREFIX;

protected:
    CustomMergerInitParam _param;
    IE_NAMESPACE(storage)::ArchiveFolderPtr _checkpointFolder;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerImpl);

}
}

#endif //ISEARCH_BS_CUSTOMMERGERIMPL_H
