#include "build_service/local_job/LocalBrokerFactory.h"
#include "build_service/task_base/JobConfig.h"
#include "build_service/local_job/LocalProcessedDocConsumer.h"
#include "build_service/local_job/LocalProcessedDocProducer.h"
#include "build_service/config/CLIOptionNames.h"
#include <autil/HashAlgorithm.h>

using namespace std;
using namespace build_service::config;
using namespace build_service::workflow;

namespace build_service {
namespace local_job {

BS_LOG_SETUP(local_job, LocalBrokerFactory);

LocalBrokerFactory::LocalBrokerFactory(ReduceDocumentQueue *queue)
    : _queue(queue)
{
}

LocalBrokerFactory::~LocalBrokerFactory() {
}

workflow::RawDocProducer *LocalBrokerFactory::createRawDocProducer(
        const RoleInitParam &initParam)
{
    return NULL;
}

workflow::RawDocConsumer *LocalBrokerFactory::createRawDocConsumer(
            const RoleInitParam &initParam)
{
    return NULL;
}

workflow::ProcessedDocProducer *LocalBrokerFactory::createProcessedDocProducer(
            const RoleInitParam &initParam)
{
    string srcSignature = getValueFromKeyValueMap(initParam.kvMap, SRC_SIGNATURE);
    uint64_t src = autil::HashAlgorithm::hashString64(
            srcSignature.c_str(), srcSignature.size());
    task_base::JobConfig jobConfig;
    if (!jobConfig.init(initParam.kvMap)) {
        string errorMsg = "create LocalProcessedDocConsumer failed : Invalid job config!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return NULL;
    }
    vector<int32_t> reduceIdTable = jobConfig.calcMapToReduceTable();
    return new LocalProcessedDocProducer(_queue, src,
            reduceIdTable[initParam.partitionId.range().from()]);
}

workflow::ProcessedDocConsumer *LocalBrokerFactory::createProcessedDocConsumer(
            const RoleInitParam &initParam)
{
    task_base::JobConfig jobConfig;
    if (!jobConfig.init(initParam.kvMap)) {
        string errorMsg = "create LocalProcessedDocConsumer failed : Invalid job config!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return NULL;
    }
    vector<int32_t> reduceIdTable = jobConfig.calcMapToReduceTable();
    return new LocalProcessedDocConsumer(_queue, reduceIdTable);
}

}
}
