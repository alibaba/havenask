#ifndef ISEARCH_SQL_EXCHANGE_KERNEL_H
#define ISEARCH_SQL_EXCHANGE_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/common/TableDistribution.h>
#include <ha3/sql/data/Table.h>
#include <ha3/service/QrsResponse.h>
#include <ha3/sql/ops/exchange/Ha3QrsSqlRequestGenerator.h>
#include <multi_call/interface/QuerySession.h>
#include <autil/mem_pool/Pool.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <kmonitor/client/MetricsReporter.h>

BEGIN_HA3_NAMESPACE(sql);

class ExchangeKernel : public navi::Kernel {
public:
    ExchangeKernel();
    ~ExchangeKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext& initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void outputResult(navi::KernelComputeContext &runContext);
    void doCompute(TablePtr input);
private:
    void fillRpcInfo(const std::vector<multi_call::ResponsePtr> &responseVec, RpcInfo *rpcInfo);

    bool fillResults(multi_call::ReplyPtr &reply, RpcInfo *rpcInfo);
    bool getSqlGraphResponse(const multi_call::ResponsePtr &response,
                             SqlGraphResponse &sqlResponse);
    bool getTableFromTensorProto(const NamedTensorProto &tensorProto, TablePtr &table);
    bool getResultFromTensorProto(const NamedTensorProto &tensorProto,
                                  navi::NaviResultPtr &result);
    bool mergeSearchInfoFromTensorProto(const NamedTensorProto &tensorProto);
    bool getDataFromResponse(
            const multi_call::ResponsePtr &response,
            const multi_call::ReplyPtr &reply);
    std::string getBizName(const std::string &dbName) const;
    void reportMetrics();
    bool patchKhronosHashInfo();
private:
    std::string _graphStr;
    std::string _inputPort;
    std::string _inputNode;
    size_t _timeout;
    std::string _traceLevel;
    uint32_t _threadLimit;
    std::string _bizName;
    std::string _tableName;
    std::string _tableType;
    std::string _sourceId;
    std::string _sourceSpec;
    TableDistribution _tableDist;
    multi_call::QuerySession *_gigQuerySession = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    std::shared_ptr<autil::mem_pool::Pool> _tablePool;
    bool _lackResultEnable;
    TablePtr _table;
    kmonitor::MetricsReporter *_queryMetricsReporter = nullptr;
    navi::MemoryPoolResource *_memoryPoolResource = nullptr;
    ExchangeInfo _exchangeInfo;
    SqlSearchInfoCollector *_globalSqlSearchInfoCollector = nullptr;
    SqlSearchInfoCollector _localSqlSearchInfoCollector;
    int32_t _opId;
private:
    HA3_LOG_DECLARE();

};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_EXCHANGE_KERNEL_H
