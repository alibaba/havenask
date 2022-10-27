#include <ha3/turing/searcher/SearcherServiceSnapshot.h>
#include <ha3/turing/searcher/SearcherBiz.h>
#include <ha3/turing/searcher/DefaultAggBiz.h>
#include <ha3/turing/searcher/ParaSearchBiz.h>
#include <ha3/turing/searcher/DefaultSqlBiz.h>
#include <ha3/version.h>
#include <autil/legacy/exception.h>
#include <multi_call/common/ControllerParam.h>
#include <ha3/common/VersionCalculator.h>
#include <ha3/util/EnvParser.h>

using namespace suez;
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);
HA3_LOG_SETUP(turing, SearcherServiceSnapshot);

SearcherServiceSnapshot::SearcherServiceSnapshot() {
}

SearcherServiceSnapshot::~SearcherServiceSnapshot() {
}

suez::BizMetas SearcherServiceSnapshot::addExtraBizMetas(
        const suez::BizMetas &currentBizMetas) const
{
    if (currentBizMetas.size() <= 0) {
        return currentBizMetas;
    }
    auto copyMetas = currentBizMetas;
    auto it = copyMetas.find(DEFAULT_BIZ_NAME);
    if (it != copyMetas.end() && _basicTuringBizNames.count(DEFAULT_BIZ_NAME) == 0) {
        if (DefaultAggBiz::isSupportAgg(_defaultAggStr)) {
            copyMetas[HA3_DEFAULT_AGG] = it->second;
            HA3_LOG(INFO, "default agg use [%s]", _defaultAggStr.c_str());
        }
        const string &disableSqlStr = sap::EnvironUtil::getEnv("disableSql", "");
        if (disableSqlStr != "true") {
            copyMetas[HA3_DEFAULT_SQL] = it->second;
            HA3_LOG(INFO, "add default sql biz.");
        }
        vector<string> paraWaysVec;
        if (EnvParser::parseParaWays(_paraWaysStr, paraWaysVec)) {
            for (const auto &paraVal : paraWaysVec) {
                copyMetas[HA3_PARA_SEARCH_PREFIX + paraVal] = it->second;
            }
            HA3_LOG(INFO, "para search use [%s]", _paraWaysStr.c_str());
        }
    }
    return copyMetas;
}

void SearcherServiceSnapshot::calculateBizMetas(
        const suez::BizMetas &currentBizMetas,
        ServiceSnapshot *oldSnapshot, suez::BizMetas &toUpdate,
        suez::BizMetas &toKeep) const
{
    const suez::BizMetas &metas = addExtraBizMetas(currentBizMetas);
    ServiceSnapshot::calculateBizMetas(metas, oldSnapshot, toUpdate, toKeep);
}

BizPtr SearcherServiceSnapshot::doCreateBiz(const string &bizName) {
    BizPtr bizPtr;
    if (_basicTuringBizNames.count(bizName) == 1) {
        bizPtr.reset(new Biz());
    } else if (bizName.find(HA3_USER_SEARCH) != string::npos) {
        bizPtr.reset(new Biz());
    } else if (StringUtil::endsWith(bizName, HA3_DEFAULT_AGG)) {
        bizPtr.reset(new DefaultAggBiz(_defaultAggStr));
    } else if (bizName.find(HA3_PARA_SEARCH_PREFIX) != string::npos) {
        bizPtr.reset(new ParaSearchBiz());
    } else if (StringUtil::endsWith(bizName, HA3_DEFAULT_SQL)) {
        bizPtr.reset(new DefaultSqlBiz());
    } else {
        bizPtr.reset(new SearcherBiz());
    }
    return bizPtr;
}

bool SearcherServiceSnapshot::doInit(const InitOptions &options,
                                     ServiceSnapshot *oldSnapshot){
    _fullVersionStr = getFullVersionStr(options.indexProvider);
    return true;
}

string SearcherServiceSnapshot::getFullVersionStr(const IndexProvider *indexProvider) const {
    string ret;
    const auto &tableReader = indexProvider->tableReader;
    try {
        for (const auto &pair : tableReader) {
            ret += ToJsonString(pair.first.tableId);
        }
    } catch (const ExceptionBase &e) {
        ret = "default hash string";
        HA3_LOG(WARN, "getFullVersionStr exception [%s]", e.what());
    }
    return ret;
}

int64_t SearcherServiceSnapshot::calcVersion(Biz *biz) {
    assert(biz);
    if(-1l != biz->getBizInfo()._version) {
        return biz->getBizInfo()._version;
    }
    return VersionCalculator::calcVersion(
        _workerParam.workerConfigVersion,
        biz->getBizInfo()._versionConfig.getDataVersion(),
        biz->getBizMeta().getRemoteConfigPath(), _fullVersionStr);
}

uint32_t SearcherServiceSnapshot::calcProtocolVersion(Biz *biz) {
    assert(biz);
    auto configProtocolVersion = biz->getBizInfo()._versionConfig.getProtocolVersion();
    string versionStr = HA_PROTOCOL_VERSION;
    if (!configProtocolVersion.empty()) {
        versionStr += ":" + configProtocolVersion;
    }
    uint32_t protocolVersion = autil::HashAlgorithm::hashString(versionStr.c_str(), versionStr.size(), 0);
    HA3_LOG(INFO, "ha3 protocol version str: [%s], version [%u]",
            versionStr.c_str(), protocolVersion);
    return protocolVersion;
}

std::string SearcherServiceSnapshot::calTopoInfo() {
    multi_call::TopoInfoBuilder infoBuilder;

    int32_t partCount = _serviceInfo.getPartCount();
    int32_t partId = _serviceInfo.getPartId();
    for (const auto &bizMapPair : _bizMap) {
        int64_t dataVersion = calcVersion(bizMapPair.second.get());
        uint32_t protocolVersion = calcProtocolVersion(bizMapPair.second.get());
        infoBuilder.addBiz(bizMapPair.first,
                           partCount,
                           partId,
                           dataVersion,
                           multi_call::MAX_WEIGHT,
                           protocolVersion,
                           _grpcPort);
    }
    return infoBuilder.build();
}

std::string SearcherServiceSnapshot::getBizName(const std::string &bizName) const{
    return getZoneBizName(bizName);
}

END_HA3_NAMESPACE(turing);
