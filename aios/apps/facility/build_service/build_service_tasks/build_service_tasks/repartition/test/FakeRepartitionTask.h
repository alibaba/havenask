#ifndef ISEARCH_BS_FAKEREPARTITIONTASK_H
#define ISEARCH_BS_FAKEREPARTITIONTASK_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/repartition/RepartitionTask.h"

namespace build_service { namespace task_base {

class FakeRepartitionTask : public RepartitionTask
{
public:
    FakeRepartitionTask(int32_t exceptionStep = 1000000000);
    ~FakeRepartitionTask();

private:
    FakeRepartitionTask(const FakeRepartitionTask&);
    FakeRepartitionTask& operator=(const FakeRepartitionTask&);

public:
    bool checkInCheckpoint(const std::string& targetGenerationPath, const config::TaskTarget& target) override;
    bool readCheckpoint(const std::string& targetGenerationPath, config::TaskTarget& checkpoint) override;
    bool doMerge(const indexlib::merger::FilteredMultiPartitionMergerPtr& merger,
                 const std::string& mergeMetaVersionDir) override;
    bool endMerge(const indexlib::merger::FilteredMultiPartitionMergerPtr& merger,
                  const std::string& mergeMetaVersionDir) override;

private:
    void maybeException();

private:
    int32_t _step;
    int32_t _exceptionStep;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeRepartitionTask);

}} // namespace build_service::task_base

#endif // ISEARCH_BS_FAKEREPARTITIONTASK_H
