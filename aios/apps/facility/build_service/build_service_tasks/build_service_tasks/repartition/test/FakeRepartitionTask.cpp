#include "build_service_tasks/repartition/test/FakeRepartitionTask.h"

#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::util;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, FakeRepartitionTask);

FakeRepartitionTask::FakeRepartitionTask(int32_t exceptionStep) : _step(0), _exceptionStep(exceptionStep) {}

FakeRepartitionTask::~FakeRepartitionTask() {}

void FakeRepartitionTask::maybeException()
{
    if (_step == _exceptionStep) {
        _step++;
        AUTIL_LEGACY_THROW(FileIOException, "fake file not exception.");
    }
    _step++;
}

bool FakeRepartitionTask::checkInCheckpoint(const std::string& targetGenerationPath, const config::TaskTarget& target)
{
    maybeException();
    bool ret = RepartitionTask::checkInCheckpoint(targetGenerationPath, target);
    maybeException();
    return ret;
}

bool FakeRepartitionTask::readCheckpoint(const std::string& targetGenerationPath, config::TaskTarget& checkpoint)
{
    maybeException();
    bool ret = RepartitionTask::readCheckpoint(targetGenerationPath, checkpoint);
    maybeException();
    return ret;
}

bool FakeRepartitionTask::doMerge(const indexlib::merger::FilteredMultiPartitionMergerPtr& merger,
                                  const std::string& mergeMetaVersionDir)
{
    maybeException();
    bool ret = RepartitionTask::doMerge(merger, mergeMetaVersionDir);
    maybeException();
    return ret;
}

bool FakeRepartitionTask::endMerge(const indexlib::merger::FilteredMultiPartitionMergerPtr& merger,
                                   const std::string& mergeMetaVersionDir)
{
    maybeException();
    bool ret = RepartitionTask::endMerge(merger, mergeMetaVersionDir);
    maybeException();
    return ret;
}

}} // namespace build_service::task_base
