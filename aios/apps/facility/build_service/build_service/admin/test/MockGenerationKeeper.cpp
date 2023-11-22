#include "build_service/admin/test/MockGenerationKeeper.h"

#include <assert.h>
#include <iosfwd>

#include "autil/LoopThread.h"
#include "build_service/admin/test/FakeGenerationTask.h"
#include "build_service/admin/test/GenerationTaskStateMachine.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/PathDefine.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, MockGenerationKeeper);

MockGenerationKeeper::~MockGenerationKeeper()
{
    // stop GenerationKeeper's loopThread
    // avoid that loopThread invokes virtual function needAutoStop in destruction
    // when MockGenerationKeeper has been destructed
    if (_loopThreadPtr) {
        _loopThreadPtr->stop();
        _loopThreadPtr.reset();
    }
}

GenerationTaskBase* MockGenerationKeeper::createGenerationTask(const BuildId& buildId, bool jobMode,
                                                               const std::string& buildMode)
{
    if (_useFakeGenerationTask) {
        string indexRoot = _fakeIndexRoot;
        string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, buildId);
        bool exist = false;
        fslib::util::FileUtil::isExist(generationDir, exist);
        if (!exist) {
            fslib::util::FileUtil::mkDir(generationDir, true);
        }
        return new FakeGenerationTask(indexRoot, buildId, _zkWrapper);
    }
    return GenerationKeeper::createGenerationTask(buildId, jobMode, buildMode);
}

GenerationTaskBase* MockGenerationKeeper::innerRecoverGenerationTask(const string& status)
{
    if (_useFakeGenerationTask) {
        // not support job mode
        return new FakeGenerationTask(_zkRoot, _buildId, _zkWrapper);
    }
    return GenerationTaskBase::recover(_buildId, status, _zkWrapper);
}

void MockGenerationKeeper::mockSuspend(const string& clusterName)
{
    if (!_useFakeGenerationTask) {
        assert(false);
    }
    FakeGenerationTask* gTask = dynamic_cast<FakeGenerationTask*>(_generationTask);
    GenerationTaskStateMachine machine("simple", _workerTable, gTask);
    machine.suspendBuilder(clusterName);
    machine.suspendProcessor();
}

}} // namespace build_service::admin
