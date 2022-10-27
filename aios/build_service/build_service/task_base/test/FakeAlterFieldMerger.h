#ifndef ISEARCH_BS_FAKEALTERFIELDMERGER_H
#define ISEARCH_BS_FAKEALTERFIELDMERGER_H

#include "build_service/custom_merger/AlterDefaultFieldMerger.h"

namespace build_service {
namespace task_base {

class FakeAlterFieldMerger : public custom_merger::AlterDefaultFieldMerger
{
public:
    FakeAlterFieldMerger();
    ~FakeAlterFieldMerger();
private:
    FakeAlterFieldMerger(const FakeAlterFieldMerger &);
    FakeAlterFieldMerger& operator=(const FakeAlterFieldMerger &);
public:
    bool merge(const custom_merger::CustomMergeMeta& mergeMeta,
               size_t instanceId,
               const std::string& path) override;
    bool endMerge(const custom_merger::CustomMergeMeta& mergeMeta,
                  const std::string& path,
                  int32_t targetVersionId) override;
    bool estimateCost(std::vector<custom_merger::CustomMergerTaskItem>& taskItems) override;

    static void setErrorStep(int step) {
        _errorStep = step;
    }
    static void init() {
        _currentStep = 0;
    }
private:
    bool doMergeTask(const custom_merger::CustomMergePlan::TaskItem& taskItem,
                     const std::string& dir) override;
    std::string prepareInstanceDir(size_t instanceId,
                                   const std::string& indexRoot) override;
    std::vector<std::string> listInstanceDirs(const std::string & rootPath) override;
    std::vector<std::string> listInstanceFiles(const std::string& instanceDir,
                                               const std::string& subDir) override;
    void moveFiles(const std::vector<std::string> &files,
                   const std::string &targetDir) override;
private:
    void maybeThrow();
private:
    static int64_t _currentStep;
    static int64_t _errorStep;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeAlterFieldMerger);

}
}

#endif //ISEARCH_BS_FAKEALTERFIELDMERGER_H
