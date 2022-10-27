#ifndef ISEARCH_SQL_SCAN_BASE_H
#define ISEARCH_SQL_SCAN_BASE_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/common/TableDistribution.h>
#include <ha3/sql/data/Table.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/sql/ops/scan/ScanIteratorCreator.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>
#include <ha3/common/TimeoutTerminator.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>

BEGIN_HA3_NAMESPACE(sql);

class ScanIteratorCreator;
class ScanIterator;
class SqlBizResource;
class SqlQueryResource;

struct NestTableAttr : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("output_fields", outputFields, outputFields);
        json.Jsonize("output_fields_type", outputFieldsType, outputFieldsType);
        json.Jsonize("table_name", tableName, tableName);
    }
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    std::string tableName;
};

struct ScanInitParam {
    ScanInitParam()
        : useNest(false)
        , batchSize(0)
        , limit(-1)
        , parallelNum(1)
        , parallelIndex(0)
        , opId(-1)
        , bizResource(nullptr)
        , memoryPoolResource(nullptr)
        , queryResource(nullptr)
    {}
    bool initFromJson(autil::legacy::Jsonizable::JsonWrapper &wrapper);

    std::string opName;
    std::string nodeName;
    std::string tableName;
    std::string dbName;
    std::string catalogName;
    std::string tableType;
    std::map<std::string, std::map<std::string, std::string> > hintsMap;
    std::map<std::string, IndexInfo> indexInfos;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    std::string hashType;
    std::vector<std::string> hashFields;
    TableDistribution tableDist;
    std::vector<NestTableAttr> nestTableAttrs;
    std::vector<std::string> usedFields;
    bool useNest;
    uint32_t batchSize;
    uint32_t limit;
    uint32_t parallelNum;
    uint32_t parallelIndex;
    int32_t opId;
    std::string conditionJson;
    std::string outputExprsJson;
    SqlBizResource* bizResource;
    navi::MemoryPoolResource *memoryPoolResource;
    SqlQueryResource* queryResource;
private:
    HA3_LOG_DECLARE();
};

struct StreamQuery {
    common::QueryPtr query;
    std::vector<std::string> primaryKeys;
    std::string toString() {
        if (query) {
            return query->toString();
        }
        std::stringstream ss;
        ss << "primaryKeys:[";
        for (size_t i = 0; i < primaryKeys.size(); ++i) {
            ss << primaryKeys[i] << ", ";
        }
        ss << "]";
        return ss.str();
    }
};
HA3_TYPEDEF_PTR(StreamQuery);

class ScanBase {
public:
    ScanBase();
    virtual ~ScanBase();
private:
    ScanBase(const ScanBase&);
    ScanBase& operator=(const ScanBase &);
public:
    virtual bool init(const ScanInitParam &param);
    bool batchScan(TablePtr &table, bool &eof);

    virtual bool updateScanQuery(const StreamQueryPtr &inputQuery) {
        return false;
    }
protected:
    virtual void patchHintInfo(const std::map<std::string, std::map<std::string, std::string> > &hintsMap);
    virtual bool doBatchScan(TablePtr &table, bool &eof) {
        return false;
    }
protected:
    void reportMetrics();
    void incInitTime(int64_t time);
    void incComputeTime();
    void incSeekTime(int64_t time);
    void incEvaluateTime(int64_t time);
    void incOutputTime(int64_t time);
    void incTotalTime(int64_t time);
    void incTotalScanCount(int64_t count);
    void incTotalSeekCount(int64_t count);
    void incTotalOutputCount(int64_t count);
    virtual std::string getReportMetricsPath();
    virtual std::string getTableNameForMetrics() {
        return _tableName;
    }
private:
    bool initExpressionCreator(const suez::turing::TableInfoPtr &tableInfo,
                               bool useSub,
                               SqlBizResource *bizResource,
                               SqlQueryResource *queryResource,
                               const std::string& tableType);

protected:
    bool _enableTableInfo;
    std::string _tableName;
    std::string _dbName;
    std::string _catalogName;
    std::string _nodeName;
    std::string _opName;
    bool _useSub;
    bool _scanOnce;
    uint32_t _batchSize;
    uint32_t _limit;
    uint32_t _seekCount;
    uint32_t _parallelNum;
    uint32_t _parallelIndex;
    int32_t _opId;
    uint64_t _batchScanBeginTime;
    std::string _conditionJson;
    std::string _outputExprsJson;
    std::string _hashFunc;
    IndexInfoMapType _indexInfos;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
    std::vector<std::string> _hashFields;
    std::vector<std::string> _usedFields;
    std::vector<NestTableAttr> _nestTableAttrs;
    common::TimeoutTerminatorPtr _timeoutTerminator;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    autil::mem_pool::Pool *_pool;
    suez::turing::TableInfoPtr _tableInfo;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    ScanInfo _scanInfo;
    navi::MemoryPoolResource *_memoryPoolResource;
    SqlSearchInfoCollector *_sqlSearchInfoCollector;
    NestTableJoinType _nestTableJoinType;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    suez::turing::FunctionProviderPtr _functionProvider;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ScanBase);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_SCAN_BASE_H
