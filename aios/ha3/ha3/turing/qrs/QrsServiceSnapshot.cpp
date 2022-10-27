#include <ha3/turing/qrs/QrsServiceSnapshot.h>
#include <ha3/turing/qrs/QrsBiz.h>
#include <ha3/turing/qrs/QrsTuringBiz.h>
#include <ha3/turing/qrs/QrsSqlBiz.h>
#include <autil/StringUtil.h>
#include <ha3/common/HaPathDefs.h>
#include <ha3/version.h>
#include <ha3/common/VersionCalculator.h>

using namespace suez;
using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, QrsServiceSnapshot);

QrsServiceSnapshot::QrsServiceSnapshot(bool enableSql)
    : _enableSql(enableSql)
{
}

QrsServiceSnapshot::~QrsServiceSnapshot() {
}

void QrsServiceSnapshot::calculateBizMetas(const suez::BizMetas &currentBizMetas,
        ServiceSnapshot *oldSnapshot, suez::BizMetas &toUpdate, suez::BizMetas &toKeep) const
{
    if (currentBizMetas.size() > 0) {
        BizMetas copyMetas = currentBizMetas;
        HA3_LOG(INFO, "biz size [%zu].", currentBizMetas.size());
        if (_enableSql && _basicTuringBizNames.count(DEFAULT_BIZ_NAME) == 0) {
            auto it = copyMetas.find(DEFAULT_BIZ_NAME);
            if (it != copyMetas.end()) {
                copyMetas[DEFAULT_SQL_BIZ_NAME] = it->second;
                HA3_LOG(INFO, "add default sql biz.");
            }
        }
        ServiceSnapshot::calculateBizMetas(copyMetas, oldSnapshot, toUpdate, toKeep);
    } else {
        ServiceSnapshot::calculateBizMetas(currentBizMetas, oldSnapshot, toUpdate, toKeep);
    }
}

BizPtr QrsServiceSnapshot::doCreateBiz(const std::string &bizName) {
    BizPtr bizPtr;
    if (_basicTuringBizNames.count(bizName) == 1) {
        bizPtr.reset(new QrsTuringBiz());
    } else if (StringUtil::endsWith(bizName, DEFAULT_SQL_BIZ_NAME)) {
        bizPtr.reset(new QrsSqlBiz());
    } else {
        bizPtr.reset(new QrsBiz());
    }
    return bizPtr;
}

uint32_t QrsServiceSnapshot::calcVersion(Biz *biz) {
    assert(biz);
    return VersionCalculator::calcVersion(
        _workerParam.workerConfigVersion,
        biz->getBizInfo()._versionConfig.getDataVersion(),
        biz->getBizMeta().getRemoteConfigPath(), "");
}

uint32_t QrsServiceSnapshot::calcProtocolVersion(Biz *biz) {
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

std::string QrsServiceSnapshot::calTopoInfo() {
    multi_call::TopoInfoBuilder infoBuilder;
    int32_t partCount = _serviceInfo.getPartCount();
    int32_t partId = _serviceInfo.getPartId();
    auto it = _bizMap.begin();
    if (it != _bizMap.end()) {
        auto qrsBiz = dynamic_cast<QrsBiz *>(it->second.get());
        if (qrsBiz) {
            uint32_t dataVersion = calcVersion((Biz *)qrsBiz);
            uint32_t protocolVersion = calcProtocolVersion((Biz *)qrsBiz);
            const vector<string> &topoBizNames = getTopoBizNames(qrsBiz);
            for (const auto &bizName : topoBizNames) {
                infoBuilder.addBiz(bizName,
                        partCount,
                        partId,
                        dataVersion,
                        multi_call::MAX_WEIGHT,
                        protocolVersion,
                        _grpcPort);
            }
        }
    }
    return infoBuilder.build();
}

// todo add default.sql
vector<string> QrsServiceSnapshot::getTopoBizNames(QrsBiz *qrsBiz) const {
    std::string domainName = "ha3";
    vector<string> searcherBizNames;
    if (qrsBiz->getConfigAdapter()) {
        qrsBiz->getConfigAdapter()->getClusterNamesWithExtendBizs(searcherBizNames);
    }
    vector<string> topoBizNames;
    for (const auto &bizName : searcherBizNames) {
        topoBizNames.push_back(domainName + "." + bizName);
    }
    return topoBizNames;
}

std::string QrsServiceSnapshot::getBizName(const std::string &bizName) const{
    if (_basicTuringBizNames.count(bizName) == 1) {
        return bizName;
    }
    return getZoneBizName(bizName);
}

bool QrsServiceSnapshot::initPid() {
    PartitionID partitionId;
    partitionId.set_clustername(getZoneName());
    util::PartitionRange range;
    if (!getRange(_serviceInfo.getPartCount(), _serviceInfo.getPartId(), range)) {
        HA3_LOG(ERROR, "range invalid [%s]", ToJsonString(_serviceInfo).c_str());
        return false;
    }
    Range protoRange;
    protoRange.set_from(range.first);
    protoRange.set_to(range.second);
    *(partitionId.mutable_range()) = protoRange;
    partitionId.set_role(ROLE_QRS);
    _pid = partitionId;
    return true;
}

bool QrsServiceSnapshot::doInit(const InitOptions &options, ServiceSnapshot *oldSnapshot) {
    initAddress();
    if(!initPid()) {
        HA3_LOG(ERROR, "init Pid failed.");
        return false;
    }
    return true;
}

void QrsServiceSnapshot::initAddress() {
    unsigned short port;
    StringUtil::fromString(WorkerParam::getEnv(WorkerParam::PORT), port);
    std::string ip = WorkerParam::getEnv(WorkerParam::IP, "");
    _address = HaPathDefs::makeWorkerAddress(ip, (unsigned short)(port));
}


bool QrsServiceSnapshot::getRange(uint32_t partCount, uint32_t partId, util::PartitionRange &range) {
    RangeVec vec = RangeUtil::splitRange(0, 65535, partCount);
    if (partId >= vec.size()) {
        return false;
    }
    range = vec[partId];
    return true;
}

QrsBizPtr QrsServiceSnapshot::getFirstQrsBiz() {
	QrsBizPtr qrsBiz;
	for (auto &iter : _bizMap) {
		qrsBiz = dynamic_pointer_cast<QrsBiz>(iter.second);
		if (qrsBiz != nullptr) {
			break;
		}
	}
	return qrsBiz;
}

QrsSqlBizPtr QrsServiceSnapshot::getFirstQrsSqlBiz() {
	QrsSqlBizPtr qrsSqlBiz;
	for (auto &iter : _bizMap) {
		qrsSqlBiz = dynamic_pointer_cast<QrsSqlBiz>(iter.second);
		if (qrsSqlBiz != nullptr) {
			break;
		}
	}
	return qrsSqlBiz;
}
 	

END_HA3_NAMESPACE(turing);

