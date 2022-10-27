#include <ha3/service/RpcContextUtil.h>
#include <arpc/ANetRPCController.h>
#include <autil/DataBuffer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, RpcContextUtil);

const char *RpcContextUtil::roleStr(proto::RoleType roleType) {
    switch (roleType) {
    case proto::ROLE_QRS:
        return "qrs";
    case proto::ROLE_PROXY:
        return "pxy";
    case proto::ROLE_SEARCHER:
        return "se";
    default:
        return "";
    }
}

std::string RpcContextUtil::getRpcRole(const proto::PartitionID &pid) {
    std::stringstream ss;
    ss << roleStr(pid.role()) << '_' << pid.clustername() << '_';
    ss << setw(5) << setfill('0') << pid.range().from() << "_";
    ss << setw(5) << setfill('0') << pid.range().to();
    return ss.str();
}

RpcContextPtr RpcContextUtil::qrsReceiveRpc(
        const string &traceid,
        const string &rpcid,
        const string &udata,
        const string &groupid,
        const string &appid,
        RPCController* controller)
{
    RpcContextPtr rpcContext = RpcContext::create(traceid, rpcid, udata);
    if (rpcContext.get()) {
        arpc::ANetRPCController *ARPCController = dynamic_cast<arpc::ANetRPCController *>(controller);
        if (ARPCController) {
            rpcContext->setRemoteIP(ARPCController->GetClientAddress());
        }
        rpcContext->setAppGroup(groupid);
        rpcContext->setAppId(appid);
        rpcContext->serverBegin("qrs", "search");
    }
    return rpcContext;
}

std::string RpcContextUtil::normAddr(const std::string &addr) {
    if (autil::StringUtil::startsWith(addr, "tcp:")) {
        return addr.substr(4);
    } else if (autil::StringUtil::startsWith(addr, "udp:")) {
        return addr.substr(4);
    } else {
        return addr;
    }
}

END_HA3_NAMESPACE(service);
