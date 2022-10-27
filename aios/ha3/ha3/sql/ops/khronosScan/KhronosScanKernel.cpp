#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/any.h>
#include <autil/StringUtil.h>
#include <ha3/sql/ops/khronosScan/KhronosScanKernel.h>
#include <ha3/sql/ops/khronosScan/KhronosMetaScan.h>
#include <ha3/sql/ops/khronosScan/KhronosDataPointScan.h>
#include <ha3/sql/ops/khronosScan/KhronosDataSeriesScan.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>

using namespace std;
using namespace suez::turing;
using namespace autil;
using namespace autil_rapidjson;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, KhronosScanKernel);


KhronosScanKernel::KhronosScanKernel()
{
}

KhronosScanKernel::~KhronosScanKernel() {
    _scanBase.reset();
}

// kernel def, see: ha3/sql/executor/proto/KernelDef.proto
const navi::KernelDef *KhronosScanKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("KhronosTableScanKernel");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool KhronosScanKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    if (!_initParam.initFromJson(json)) {
        return false;
    }
    std::map<std::string, std::map<std::string, std::string> > hintsMap;
    json.Jsonize("hints", hintsMap, hintsMap);
    patchHintInfo(hintsMap);
    return true;
}

navi::ErrorCode KhronosScanKernel::init(navi::KernelInitContext& context) {
    _initParam.opName = getKernelName();
    _initParam.nodeName = getNodeName();
    _initParam.memoryPoolResource = context.getResource<navi::MemoryPoolResource>(
        navi::RESOURCE_MEMORY_POOL_URI);
    if (!_initParam.memoryPoolResource) {
        SQL_LOG(ERROR, "get mem pool resource failed");
        return navi::EC_ABORT;
    }

    _initParam.bizResource = context.getResource<SqlBizResource>("SqlBizResource");
    KERNEL_REQUIRES(_initParam.bizResource, "get sql biz resource failed");

    _initParam.queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(_initParam.queryResource, "get sql query resource failed");

    _accessLog.reset(new KhronosScanAccessLog);
    if (_initParam.tableType == KHRONOS_META_TABLE_TYPE) {
        SQL_LOG(TRACE2, "use KhronosMetaScan");
        _scanBase.reset(new KhronosMetaScan(_scanHints, _accessLog.get()));
    } else if (_initParam.tableType == KHRONOS_DATA_POINT_TABLE_TYPE) {
        SQL_LOG(TRACE2, "use KhronosDataPointScan");
        _scanBase.reset(new KhronosDataPointScan(_scanHints, _accessLog.get()));
    } else if (_initParam.tableType == KHRONOS_DATA_SERIES_TABLE_TYPE) {
        SQL_LOG(TRACE2, "use KhronosDataSeriesScan");
        _scanBase.reset(new KhronosDataSeriesScan(_scanHints, _accessLog.get()));
    }
    if (_scanBase == nullptr) {
        SQL_LOG(ERROR, "not support table type [%s].", _initParam.tableType.c_str());
        return navi::EC_ABORT;
    }
    if (!_scanBase->init(_initParam)) {
        SQL_LOG(ERROR, "init khronos scan kernel failed, table [%s].",
                        _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

navi::ErrorCode KhronosScanKernel::compute(navi::KernelComputeContext &context) {
    TablePtr table;
    bool eof = true;
    if (_scanBase) {
        if (!_scanBase->batchScan(table, eof)) {
            SQL_LOG(ERROR, "batch scan failed");
            return navi::EC_ABORT;
        }
    }
    navi::PortIndex index(0, navi::INVALID_INDEX);
    context.setOutput(index, table, eof);
    return navi::EC_NONE;
}

void KhronosScanKernel::patchHintInfo(const map<string, map<string, string> > &hintsMap) {
    const auto &mapIter = hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const map<string, string> &hints = mapIter->second;
    auto iter = hints.find("khronosOneLineScanPointLimit");
    if (iter != hints.end()) {
        size_t limit = 0;
        StringUtil::fromString(iter->second, limit);
        if (limit > 0) {
            _scanHints.oneLineScanPointLimit = limit;
        }
    }
}

REGISTER_KERNEL(KhronosScanKernel);

END_HA3_NAMESPACE(sql);
