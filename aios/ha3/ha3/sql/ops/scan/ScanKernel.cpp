#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/any.h>
#include <autil/StringUtil.h>
#include <ha3/sql/ops/scan/ScanKernel.h>
#include <ha3/sql/ops/scan/NormalScan.h>
#include <ha3/sql/ops/scan/SummaryScan.h>
#include <ha3/sql/ops/scan/LogicalScan.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace suez::turing;
using namespace autil_rapidjson;
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ScanKernel);
ScanKernel::ScanKernel()
{
}

ScanKernel::~ScanKernel() {
    _scanBase.reset();
}

// kernel def, see: ha3/sql/executor/proto/KernelDef.proto
const navi::KernelDef *ScanKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("ScanKernel");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool ScanKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    if (!_initParam.initFromJson(json)) {
        return false;
    }
    KernelUtil::stripName(_initParam.outputFields);
    KernelUtil::stripName(_initParam.indexInfos);
    KernelUtil::stripName(_initParam.usedFields);
    KernelUtil::stripName(_initParam.hashFields);
    return true;
}

navi::ErrorCode ScanKernel::init(navi::KernelInitContext& context) {
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

    if (_initParam.tableType == SCAN_NORMAL_TABLE_TYPE) {
        _scanBase.reset(new NormalScan());
    } else if (_initParam.tableType == SCAN_SUMMARY_TABLE_TYPE) {
        _scanBase.reset(new SummaryScan());
    } else if (_initParam.tableType == SCAN_LOGICAL_TABLE_TYPE) {
        _scanBase.reset(new LogicalScan());
    } else {
        SQL_LOG(ERROR, "not support table type [%s].", _initParam.tableType.c_str());
    }
    if (_scanBase == nullptr || !_scanBase->init(_initParam)) {
        SQL_LOG(ERROR, "init scan kernel failed, table [%s].",
                        _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

navi::ErrorCode ScanKernel::compute(navi::KernelComputeContext &context) {
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



REGISTER_KERNEL(ScanKernel);

END_HA3_NAMESPACE(sql);
