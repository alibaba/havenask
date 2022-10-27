#ifndef ISEARCH_HA3QRSREQUESTGENERATOR_H
#define ISEARCH_HA3QRSREQUESTGENERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/DocIdClause.h>
#include <ha3/util/RangeUtil.h>
#include <ha3/config/QrsConfig.h>
#include <suez/turing/proto/Search.pb.h>
#include <multi_call/interface/RequestGenerator.h>

BEGIN_HA3_NAMESPACE(turing);

enum GenerateType {
    GT_NORMAL,
    GT_PID,
    GT_SUMMARY,
};

class Ha3QrsRequestGenerator : public multi_call::RequestGenerator
{
public:
    Ha3QrsRequestGenerator(const std::string &bizName,
                       multi_call::SourceIdTy sourceId,
                       multi_call::VersionTy version,
                       GenerateType type,
                       bool useGrpc,
                       common::RequestPtr requestPtr,
                       int32_t timeout,
                       common::DocIdClause *docIdClause,
                       const config::SearchTaskQueueRule &searchTaskqueueRule,
                       autil::mem_pool::Pool *pool,
                       HaCompressType compressType,
                       const std::shared_ptr<google::protobuf::Arena> &arena);
    ~Ha3QrsRequestGenerator();
private:
    Ha3QrsRequestGenerator(const Ha3QrsRequestGenerator &);
    Ha3QrsRequestGenerator& operator=(const Ha3QrsRequestGenerator &);
    void generate(multi_call::PartIdTy partCnt,
                  multi_call::PartRequestMap &requestMap) override;
private:
    void generateNormal(multi_call::PartIdTy partCnt,
                        multi_call::PartRequestMap &requestMap) const;
    void generatePid(multi_call::PartIdTy partCnt,
                     multi_call::PartRequestMap &requestMap) const;
    void generateSummary(multi_call::PartIdTy partCnt,
                         multi_call::PartRequestMap &requestMap) const;
    void prepareRequest(const std::string &bizName,
                        suez::turing::GraphRequest &graphRequest,
                        bool needClone) const;
    common::RequestPtr getRequest() const {
        return _requestPtr;
    }
    common::ClusterClause *getClusterClause() const {
        return _requestPtr->getClusterClause();
    }
    common::DocIdClause *getDocIdClause() const {
        return _docIdClause;
    }
    void setDocIdClause(common::DocIdClause *docIdClause) {
        _docIdClause = docIdClause;
    }
    multi_call::RequestPtr createQrsRequest(
            const std::string &methodName,
            suez::turing::GraphRequest *graphRequest) const;
private:
    void makeDebugRunOptions(suez::turing::GraphRequest &graphRequest) const;
private:
    static std::set<int32_t> getPartIds(const HA3_NS(util)::RangeVec& ranges,
            const HA3_NS(util)::RangeVec& hashIds);
    static void getPartId(const HA3_NS(util)::RangeVec& ranges,
                          const HA3_NS(util)::PartitionRange &range,
                          std::set<int32_t> &partIds);
private:
    GenerateType _generateType;
    bool _useGrpc;
    common::RequestPtr _requestPtr;
    int32_t _timeout;
    common::DocIdClause *_docIdClause;
    const config::SearchTaskQueueRule &_searchTaskqueueRule;
    autil::mem_pool::Pool *_pool;
    HaCompressType _compressType;
private:
    static std::string SEARCH_METHOD_NAME;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Ha3QrsRequestGenerator);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_HA3QRSREQUESTGENERATOR_H
