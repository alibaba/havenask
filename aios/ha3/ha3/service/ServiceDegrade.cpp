#include <ha3/service/ServiceDegrade.h>
#include <autil/TimeUtility.h>
using namespace autil;

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, ServiceDegrade);

ServiceDegrade::ServiceDegrade(const config::ServiceDegradationConfig &config) {
    _config = config;
}

ServiceDegrade::~ServiceDegrade() {
}

bool ServiceDegrade::updateRequest(common::Request *request,
                                   const multi_call::QueryInfoPtr &queryInfo)
{
    if (!queryInfo) {
        return false;
    }
    auto *configClause = request->getConfigClause();
    auto level = queryInfo->degradeLevel(multi_call::MAX_PERCENT);
    auto ret = level > multi_call::MIN_PERCENT;
    if (ret && _config.request.rankSize > 0) {
        configClause->setTotalRankSize(0);
    }
    if (ret && _config.request.rerankSize > 0) {
        configClause->setTotalRerankSize(0);
    }
    request->setDegradeLevel(level, _config.request.rankSize,
                             _config.request.rerankSize);
    return ret;
}

END_HA3_NAMESPACE(service);
