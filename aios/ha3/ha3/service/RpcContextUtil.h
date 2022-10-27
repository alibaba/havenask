#ifndef ISEARCH_RPCCONTEXTUTIL_H
#define ISEARCH_RPCCONTEXTUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <sap_eagle/RpcContext.h>
#include <arpc/CommonMacros.h>
#include <multi_call/common/common.h>

BEGIN_HA3_NAMESPACE(service);

#define HA_RESERVED_METHOD "ha_reserved_method"
#define HA_RESERVED_PID "ha_reserved_pid"
#define HA_RESERVED_APPNAME "ha3:"
#define HA_RESERVED_BENCHMARK_KEY "t"
#define HA_RESERVED_BENCHMARK_VALUE_1 "1"
#define HA_RESERVED_BENCHMARK_VALUE_2 "2"

typedef sap::RpcContext RpcContext;
typedef sap::RpcContextPtr RpcContextPtr;

class RpcContextUtil
{
public:
    static const char *roleStr(proto::RoleType roleType);
    static std::string getRpcRole(const proto::PartitionID &pid);

    static RpcContextPtr qrsReceiveRpc(
            const std::string &traceid,
            const std::string &rpcid,
            const std::string &udata,
            const std::string &groupid,
            const std::string &appid,
            RPCController* controller);

    static std::string normAddr(const std::string &addr);
private:
    RpcContextUtil();
    ~RpcContextUtil();

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RpcContextUtil);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_RPCCONTEXTUTIL_H
