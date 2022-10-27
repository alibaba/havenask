#include <ha3/sql/ops/exchange/Ha3QrsSqlRequestGenerator.h>
#include <ha3/turing/ops/sql/proto/SqlSearch.pb.h>
#include <ha3/service/QrsRequest.h>
#include <ha3/common/CommonDef.h>
#include <ha3/sql/common/common.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/types.pb.h>
#include <tensorflow/core/framework/tensor_shape.h>
#include <google/protobuf/util/json_util.h>
#include <autil/StringUtil.h>
#include <autil/HashFuncFactory.h>

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace multi_call;

USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, Ha3QrsSqlRequestGenerator);
multi_call::RandomGenerator Ha3QrsSqlRequestGenerator::_randomGenerator(TimeUtility::currentTime());

Ha3QrsSqlRequestGenerator::Ha3QrsSqlRequestGenerator(const Ha3QrsSqlRequestGeneratorParam &param)
    : RequestGenerator(param.bizName, param.sourceId)
    , _graphStr(param.graphStr)
    , _inputPort(param.inputPort)
    , _inputNode(param.inputNode)
    , _timeout(param.timeout)
    , _traceLevel(param.traceLevel)
    , _threadLimit(param.threadLimit)
    , _tableDist(param.tableDist)
    , _sourceSpec(param.sourceSpec)
{
}

set<int32_t> Ha3QrsSqlRequestGenerator::genAccessPart(int32_t partCnt,
        const TableDistribution &tableDist)
{
    if (!tableDist.debugHashValues.empty() || !tableDist.debugPartIds.empty()) {
        const auto &ret =  genDebugAccessPart(partCnt, tableDist);
        SQL_LOG(DEBUG, "use debug part [%s] to access",
                StringUtil::toString(ret.begin(), ret.end(), ",").c_str());
        return ret;
    }
    if (tableDist.hashValues.empty() || tableDist.hashMode._hashFields.empty()) {
        if (partCnt > 1 && tableDist.partCnt == 1) {
            int32_t partId = _randomGenerator.get() % partCnt;
            SQL_LOG(INFO, "random select part id [%d] to access", partId);
            return {partId};
        } else {
            return genAllPart(partCnt);
        }
    } else {
        const string &hashField = tableDist.hashMode._hashFields[0];
        auto iter = tableDist.hashValues.find(hashField);
        if (iter == tableDist.hashValues.end()) {
            if (partCnt > 1 && tableDist.partCnt == 1) {
                int32_t partId = _randomGenerator.get() % partCnt;
                SQL_LOG(INFO, "random select part id [%d] to access", partId);
                return {partId};
            } else {
                return genAllPart(partCnt);
            }
        } else {
            if (partCnt > 1 && tableDist.partCnt == 1) {
                int32_t partId = _randomGenerator.get() % partCnt;
                SQL_LOG(INFO, "random select part id [%d] to access", partId);
                return {partId};
            }
            if (partCnt != tableDist.partCnt) {
                SQL_LOG(WARN, "access part count not equal, expect [%d] actual [%d]",
                        tableDist.partCnt, partCnt);
            }
            autil::HashFunctionBasePtr hashFunc = createHashFunc(tableDist.hashMode);
            if (!hashFunc) {
                return genAllPart(partCnt);
            }
            const auto &partRanges = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCnt);
            RangeVec rangeVec;
            auto sepIter = tableDist.hashValuesSep.find(iter->first);
            string valSep = sepIter == tableDist.hashValuesSep.end() ? "" : sepIter->second;
            vector<string> hashStrVec;
            vector<string> sepStrVec;
            for (auto &hashStr : iter->second) {
                if (valSep.empty()) {
                    hashStrVec = {hashStr};
                    auto hashIds = hashFunc->getHashRange(hashStrVec);
                    rangeVec.insert(rangeVec.end(), hashIds.begin(), hashIds.end());
                } else {
                    sepStrVec.clear();
                    StringUtil::split(sepStrVec, hashStr, valSep);
                    for (auto &sepStr : sepStrVec) {
                        hashStrVec = {sepStr};
                        auto hashIds = hashFunc->getHashRange(hashStrVec);
                        rangeVec.insert(rangeVec.end(), hashIds.begin(), hashIds.end());
                    }
                }
            }
            const set<int32_t> &idSet = getPartIds(partRanges, rangeVec);
            if (idSet.empty()) {
                SQL_LOG(WARN, "gen result is empty, use full access.");
                return genAllPart(partCnt);
            } else {
                return idSet;
            }
        }
    }
}

set<int32_t> Ha3QrsSqlRequestGenerator::genAllPart(int32_t partCnt) {
    set<int32_t> partSet;
    for (int32_t i = 0; i < partCnt; i++) {
        partSet.insert(i);
    }
    return partSet;
}

autil::HashFunctionBasePtr Ha3QrsSqlRequestGenerator::createHashFunc(const HashMode &hashMode) {
    if (!hashMode.validate()) {
        SQL_LOG(WARN, "validate hash mode failed.");
        return HashFunctionBasePtr();
    }
    string hashFunc = hashMode._hashFunction;
    StringUtil::toUpperCase(hashFunc);
    autil::HashFunctionBasePtr hashFunctionBasePtr =
        HashFuncFactory::createHashFunc(hashFunc,
                hashMode._hashParams, MAX_PARTITION_COUNT);
    if (!hashFunctionBasePtr) {
        HA3_LOG(WARN, "invalid hash function [%s].", hashFunc.c_str());
        return HashFunctionBasePtr();
    }
    return hashFunctionBasePtr;
}

set<int32_t> Ha3QrsSqlRequestGenerator::getPartIds(const RangeVec& ranges,
        const RangeVec &hashIds)
{
    set<int32_t> partIds;
    for (size_t i = 0; i < hashIds.size(); ++i) {
        getPartId(ranges, hashIds[i], partIds);
    }
    return partIds;
}

void Ha3QrsSqlRequestGenerator::getPartId(const RangeVec& ranges,
        const PartitionRange &hashId, set<int32_t> &partIds)
{
    for (size_t i = 0; i < ranges.size(); ++i) {
        if (!(ranges[i].second < hashId.first || ranges[i].first > hashId.second)) {
            partIds.insert(i);
        }
    }
}

void Ha3QrsSqlRequestGenerator::generate(multi_call::PartIdTy partCnt,
        multi_call::PartRequestMap &requestMap)
{
    auto arena = getProtobufArena().get();
    auto graphRequest0 =
        google::protobuf::Arena::CreateMessage<suez::turing::GraphRequest>(arena);
    const auto &bizName = getBizName();
    graphRequest0->set_bizname(bizName);
    graphRequest0->set_src(_sourceSpec);

    prepareRequest(*graphRequest0);

    auto partIds = genAccessPart(partCnt, _tableDist);
    _partIdSet = partIds;
    for (auto partId : partIds) {
        requestMap[partId] = createQrsRequest(EXCHANGE_SEARCH_METHOD_NAME, graphRequest0);
    }
}

set<int32_t> Ha3QrsSqlRequestGenerator::genDebugAccessPart(int32_t partCnt,
        const TableDistribution &tableDist)
{
    const string &hashValues = tableDist.debugHashValues;
    const string &partIds = tableDist.debugPartIds;
    if (!partIds.empty()) {
        set<int32_t> partSet;
        const vector<string> &idVec = StringUtil::split(partIds, ",");
        for (const auto &idStr : idVec) {
            int32_t id;
            if (StringUtil::numberFromString(idStr, id)) {
                if (id >=0 && id < partCnt) {
                    partSet.insert(id);
                }
            }
        }
        if (partSet.empty()) {
            return genAllPart(partCnt);
        } else {
            return partSet;
        }
    }
    if (!hashValues.empty()) {
        autil::HashFunctionBasePtr hashFunc = createHashFunc(tableDist.hashMode);
        if (!hashFunc) {
            return genAllPart(partCnt);
        }
        const auto &partRanges = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCnt);
        RangeVec rangeVec;
        const vector<string> &hashVec = StringUtil::split(hashValues, ",");
        for (const auto &hashStr : hashVec) {
            vector<string> hashStrVec = {hashStr};
            auto hashIds = hashFunc->getHashRange(hashStrVec);
            rangeVec.insert(rangeVec.end(), hashIds.begin(), hashIds.end());
        }
        const set<int32_t> &idSet = getPartIds(partRanges, rangeVec);
        if (idSet.empty()) {
            return genAllPart(partCnt);
        } else {
            return idSet;
        }
    }
    return genAllPart(partCnt);
}

void Ha3QrsSqlRequestGenerator::prepareRequest(suez::turing::GraphRequest &graphRequest) const
{
    suez::turing::GraphInfo &graphInfo = *graphRequest.mutable_graphinfo();

    suez::turing::NamedTensorProto &inputRequest = *graphInfo.add_inputs();

    auto inputRequestTensor = Tensor(DT_STRING, TensorShape({}));
    inputRequestTensor.scalar<std::string>()() = createSqlGraphRequestStr();
    inputRequest.set_name("sql_request");
    inputRequestTensor.AsProtoField(inputRequest.mutable_tensor());

    *graphInfo.add_fetches() = "run_sql";
    *graphInfo.add_targets() = "sql_done";
}

std::string Ha3QrsSqlRequestGenerator::createSqlGraphRequestStr() const {
    SqlGraphRequest request;
    auto graphInfo = request.mutable_graphinfo();
    auto graphDef = graphInfo->mutable_graphdef();
    if (!graphDef->ParseFromString(_graphStr)) {
        SQL_LOG(WARN, "parse graph string to graph def failed");
        return "";
    }
    auto target = graphInfo->add_targets();
    target->set_name(_inputNode);
    target->set_port(_inputPort);
    auto params = graphInfo->mutable_rungraphparams();
    (*params)[SUB_GRAPH_RUN_PARAM_TIMEOUT] = autil::StringUtil::toString(_timeout);
    (*params)[SUB_GRAPH_RUN_PARAM_TRACE_LEVEL] = _traceLevel;
    (*params)[SUB_GRAPH_RUN_PARAM_THREAD_LIMIT] = autil::StringUtil::toString(_threadLimit);
    string requestStr;
    request.SerializeToString(&requestStr);
    return requestStr;
}

multi_call::RequestPtr Ha3QrsSqlRequestGenerator::createQrsRequest(
        const string &methodName,
        suez::turing::GraphRequest *graphRequest) const
{
    auto qrsRequest = new service::QrsRequest(methodName, graphRequest,
                                              _timeout, getProtobufArena());
    return multi_call::RequestPtr(qrsRequest);
}

END_HA3_NAMESPACE(sql);
