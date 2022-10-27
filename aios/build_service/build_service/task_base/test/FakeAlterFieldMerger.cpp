#include "build_service/task_base/test/FakeAlterFieldMerger.h"
#include <indexlib/misc/exception.h>

using namespace std;
using namespace build_service::custom_merger;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, FakeAlterFieldMerger);

int64_t FakeAlterFieldMerger::_currentStep = 0;
int64_t FakeAlterFieldMerger::_errorStep= 0;
FakeAlterFieldMerger::FakeAlterFieldMerger(){
}

FakeAlterFieldMerger::~FakeAlterFieldMerger() {
}

void FakeAlterFieldMerger::maybeThrow() {
    _currentStep ++;
    if (_currentStep == _errorStep) {
        INDEXLIB_THROW(FileIOException,
                       "fake file not exception.");
    }
}

bool FakeAlterFieldMerger::estimateCost(std::vector<CustomMergerTaskItem>& taskItems) 
{
    maybeThrow();
    bool ret = AlterDefaultFieldMerger::estimateCost(taskItems);
    maybeThrow();
    return ret;
}

bool FakeAlterFieldMerger::merge(const CustomMergeMeta& mergeMeta,
                                 size_t instanceId,
                                 const string& indexPath)
{
    maybeThrow();
    bool ret = AlterDefaultFieldMerger::merge(mergeMeta, instanceId, indexPath);
    maybeThrow();
    return ret;
}

bool FakeAlterFieldMerger::endMerge(const CustomMergeMeta& mergeMeta,
                                    const string& path,
                                    int32_t targetVersionId) {
    maybeThrow();
    auto ret = AlterDefaultFieldMerger::endMerge(mergeMeta, path, targetVersionId);
    maybeThrow();
    return ret;
}

bool FakeAlterFieldMerger::doMergeTask(const CustomMergePlan::TaskItem& taskItem,
                                       const string& dir) {
    maybeThrow();
    auto ret = AlterDefaultFieldMerger::doMergeTask(taskItem, dir);
    maybeThrow();
    return ret;
}

string FakeAlterFieldMerger::prepareInstanceDir(size_t instanceId,
        const string& indexRoot) {
    maybeThrow();
    auto ret = AlterDefaultFieldMerger::prepareInstanceDir(instanceId, indexRoot);
    maybeThrow();
    return ret;
}

vector<string> FakeAlterFieldMerger::listInstanceDirs(const string & rootPath) {
    maybeThrow();
    auto ret = AlterDefaultFieldMerger::listInstanceDirs(rootPath);
    maybeThrow();
    return ret;
}

vector<string> FakeAlterFieldMerger::listInstanceFiles(const string& instanceDir,
                                 const string& subDir) {
    maybeThrow();
    auto ret = AlterDefaultFieldMerger::listInstanceFiles(instanceDir, subDir);
    maybeThrow();
    return ret;
}

void FakeAlterFieldMerger::moveFiles(const vector<string> &files,
                                     const string &targetDir) {
    maybeThrow();
    AlterDefaultFieldMerger::moveFiles(files, targetDir);
    maybeThrow();
}

}
}
