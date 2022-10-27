#ifndef ISEARCH_HA3QRSSQLREQUESTGENERATOR_H
#define ISEARCH_HA3QRSSQLREQUESTGENERATOR_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/common/TableDistribution.h>
#include <ha3/util/Log.h>
#include <ha3/util/RangeUtil.h>
#include <ha3/turing/ops/sql/proto/SqlSearch.pb.h>
#include <suez/turing/proto/Search.pb.h>
#include <multi_call/interface/RequestGenerator.h>
#include <autil/HashFunctionBase.h>
#include <multi_call/util/RandomGenerator.h>

BEGIN_HA3_NAMESPACE(sql);

struct Ha3QrsSqlRequestGeneratorParam {
    std::string bizName;
    multi_call::SourceIdTy sourceId;
    std::string graphStr;
    std::string inputPort;
    std::string inputNode;
    int64_t timeout;
    std::string traceLevel;
    uint32_t threadLimit;
    TableDistribution tableDist;
    std::string sourceSpec;
};

class Ha3QrsSqlRequestGenerator : public multi_call::RequestGenerator
{
public:
    Ha3QrsSqlRequestGenerator(const Ha3QrsSqlRequestGeneratorParam &param);
    ~Ha3QrsSqlRequestGenerator() {}
private:
    Ha3QrsSqlRequestGenerator(const Ha3QrsSqlRequestGenerator &);
    Ha3QrsSqlRequestGenerator& operator=(const Ha3QrsSqlRequestGenerator &);

public:
    const std::set<int32_t> & getPartIdSet() { return _partIdSet; }

private:
    void generate(multi_call::PartIdTy partCnt,
                  multi_call::PartRequestMap &requestMap) override;
    void prepareRequest(suez::turing::GraphRequest &graphRequest) const;
    std::string createSqlGraphRequestStr() const;
    multi_call::RequestPtr createQrsRequest(const std::string &methodName,
            suez::turing::GraphRequest *graphRequest) const;
private:
    static autil::HashFunctionBasePtr createHashFunc(const HashMode &hashMode);
    static std::set<int32_t> genAccessPart(int32_t partCnt, const TableDistribution &tableDist);
    static std::set<int32_t> genDebugAccessPart(int32_t partCnt, const TableDistribution &tableDist);
    static std::set<int32_t> genAllPart(int32_t partCnt);
    static std::set<int32_t> getPartIds(const HA3_NS(util)::RangeVec& ranges,
                                        const HA3_NS(util)::RangeVec &hashIds);
    static void getPartId(const HA3_NS(util)::RangeVec& ranges,
                          const HA3_NS(util)::PartitionRange &hashId,
                          std::set<int32_t> &partIds);

private:
    std::string _graphStr;
    std::string _inputPort;
    std::string _inputNode;
    int64_t _timeout;
    std::string _traceLevel;
    uint32_t _threadLimit;
    std::set<int32_t> _partIdSet;
    TableDistribution _tableDist;
    std::string _sourceSpec;
    static  multi_call::RandomGenerator _randomGenerator;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Ha3QrsSqlRequestGenerator);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HA3QRSSQLREQUESTGENERATOR_H
