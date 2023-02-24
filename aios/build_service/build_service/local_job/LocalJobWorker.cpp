#include "build_service/local_job/LocalJobWorker.h"
#include "build_service/local_job/LocalBrokerFactory.h"
#include "build_service/local_job/BuildInstanceWorkItem.h"
#include "build_service/local_job/MergeInstanceWorkItem.h"
#include "build_service/local_job/ReduceDocumentQueue.h"
#include "build_service/task_base/TaskDefine.h"
#include "build_service/task_base/ConfigDownloader.h"
#include "build_service/util/LogSetupGuard.h"
#include "build_service/config/CLIOptionNames.h"
#include <autil/StringUtil.h>
#include <autil/ThreadPool.h>
#include <autil/legacy/jsonizable.h>
#include <iostream>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::task_base;
using namespace build_service::config;

namespace build_service {
namespace local_job {

BS_LOG_SETUP(local_job, LocalJobWorker);

LocalJobWorker::LocalJobWorker() {
}

LocalJobWorker::~LocalJobWorker() {
}

bool LocalJobWorker::run(const string &step, uint16_t mapCount,
                         uint16_t reduceCount, const string &jobParams)
{
    BS_LOG(INFO, "mapCount[%u] reduceCount[%u], jobParams:%s", mapCount, reduceCount, jobParams.c_str());

    if ("build" == step) {
        return buildJob(mapCount, reduceCount, jobParams);
    } else if ("merge" == step) {
        return mergeJob(mapCount, reduceCount, jobParams);
    } else if ("endmerge" == step) {
        return endMergeJob(mapCount, reduceCount, jobParams);
    } else {
        string errorMsg = "unrecognized job step [" + step + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
}

bool LocalJobWorker::buildJob(uint16_t mapCount, uint16_t reduceCount,
                              const string &jobParams)
{
    if (!downloadConfig(jobParams)) {
        return false;
    }
    ReduceDocumentQueue queue(reduceCount);
    bool failflag = false;

    ThreadPool mapThreadPool(mapCount);
    mapThreadPool.start();
    for (uint16_t instanceId = 0; instanceId < mapCount; instanceId++) {
        BuildInstanceWorkItem *workitem = new BuildInstanceWorkItem(
                &failflag, TaskBase::BUILD_MAP, instanceId, jobParams, &queue);
        mapThreadPool.pushWorkItem(workitem, true);
    }

    ThreadPool reduceThreadPool(reduceCount);
    reduceThreadPool.start();
    for (uint16_t instanceId = 0; instanceId < reduceCount; instanceId++) {
        BuildInstanceWorkItem *workitem = new BuildInstanceWorkItem(
                &failflag, TaskBase::BUILD_REDUCE, instanceId, jobParams, &queue);
        reduceThreadPool.pushWorkItem(workitem, true);
    }

    mapThreadPool.stop();
    queue.setFinished();
    reduceThreadPool.stop();

    if (failflag) {
        return false;
    }

    return true;
}

bool LocalJobWorker::mergeJob(uint16_t mapCount, uint16_t reduceCount, const string &jobParams) {
    if (!downloadConfig(jobParams)) {
        return false;
    }
    return parallelRunMergeTask(mapCount, jobParams, TaskBase::MERGE_MAP)
        && parallelRunMergeTask(reduceCount, jobParams, TaskBase::MERGE_REDUCE);
}

bool LocalJobWorker::endMergeJob(uint16_t mapCount, uint16_t reduceCount, const string &jobParams) {
    if (!downloadConfig(jobParams)) {
        return false;
    }
    return parallelRunMergeTask(mapCount, jobParams, TaskBase::END_MERGE_MAP)
        && parallelRunMergeTask(reduceCount, jobParams, TaskBase::END_MERGE_REDUCE);
}

bool LocalJobWorker::parallelRunMergeTask(size_t instanceCount,
        const string &jobParams, TaskBase::Mode mode)
{

    bool failflag = false;
    ThreadPool threadPool(1);
    threadPool.start();
    for (uint16_t instanceId = 0; instanceId < instanceCount; instanceId++) {
        MergeInstanceWorkItem *workitem = new MergeInstanceWorkItem(
                &failflag, mode, instanceId, jobParams);
        threadPool.pushWorkItem(workitem, true);
    }
    threadPool.stop();

    return !failflag;
}

bool LocalJobWorker::downloadConfig(const std::string &jobParams) {
    KeyValueMap kvMap;
    try {
        FromJsonString(kvMap, jobParams);
    } catch (const ExceptionBase &e) {
        string errorMsg = "Invalid job param[" + jobParams
                          +"], from json failed[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
    string configPath = getValueFromKeyValueMap(kvMap, CONFIG_PATH);
    string localConfigPath;
    ConfigDownloader::DownloadErrorCode errorCode =
        ConfigDownloader::downloadConfig(configPath, localConfigPath);
    if (errorCode != ConfigDownloader::DEC_NONE) {
        string errorMsg = "download config[" + configPath + "] to local failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

}
}
