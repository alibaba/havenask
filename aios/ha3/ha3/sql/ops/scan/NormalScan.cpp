#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <autil/legacy/any.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/sql/ops/scan/NormalScan.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/scan/Ha3ScanConditionVisitor.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/util/TypeTransformer.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <ha3/common/AndQuery.h>
#include <ha3/sql/ops/calc/CalcTable.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace autil_rapidjson;
using namespace autil;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, NormalScan);

NormalScan::NormalScan()
{

}

NormalScan::~NormalScan() {
    _scanIter.reset();
    _attributeExpressionVec.clear();
    _indexPartitionReaderWrapper.reset();
}

bool NormalScan::init(const ScanInitParam &param) {
    uint64_t initBegin = TimeUtility::currentTime();
    if (!ScanBase::init(param)) {
        return false;
    }
    SqlBizResource* bizResource = param.bizResource;
    if (!bizResource) {
        SQL_LOG(ERROR, "table [%s] get sql biz resource failed.", _tableName.c_str());
        return false;
    }
    SqlQueryResource* queryResource = param.queryResource;
    if (!queryResource) {
        SQL_LOG(ERROR, "table [%s] get sql query resource failed.", _tableName.c_str());
        return false;
    }
    if (!initOutputColumn()) {
        SQL_LOG(WARN, "table [%s] init scan column failed.", _tableName.c_str());
        return false;
    }
    auto sqlSessionResource =
        bizResource->getObject<turing::SqlSessionResource>("SqlSessionResource");

    ScanIteratorCreatorParam iterParam;
    iterParam.tableName = _tableName;
    iterParam.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    iterParam.attributeExpressionCreator = _attributeExpressionCreator;
    iterParam.matchDocAllocator = _matchDocAllocator;
    iterParam.tableInfo = _tableInfo;
    iterParam.pool = _pool;
    iterParam.timeoutTerminator = _timeoutTerminator.get();
    iterParam.parallelIndex = _parallelIndex;
    iterParam.parallelNum = _parallelNum;
    if (sqlSessionResource != NULL) {
        iterParam.analyzerFactory = sqlSessionResource->analyzerFactory.get();
        iterParam.queryInfo = &sqlSessionResource->queryInfo;
    } else {
        SQL_LOG(INFO, "table [%s] get sql session resource failed.", _tableName.c_str());
    }
    _scanIterCreator.reset(new ScanIteratorCreator(iterParam));
    bool emptyScan = false;
    if (!_scanIterCreator->genCreateScanIteratorInfo(_conditionJson, _indexInfos, _baseCreateScanIterInfo)) {
        SQL_LOG(WARN, "table [%s] generate create scan iterator info failed", _tableName.c_str());
        return false;
    }
    _scanIter.reset(_scanIterCreator->createScanIterator(_baseCreateScanIterInfo, emptyScan));
    if (_scanIter == NULL && !emptyScan) {
        SQL_LOG(WARN, "table [%s] create ha3 scan iterator failed", _tableName.c_str());
        return false;
    }
    if (_batchSize == 0) {
        uint32_t docSize = _matchDocAllocator->getDocSize();
        docSize += _matchDocAllocator->getSubDocSize();
        if (docSize == 0) {
            _batchSize = DEFAULT_BATCH_COUNT;
        } else {
            _batchSize = DEFAULT_BATCH_SIZE / docSize;
            if (_batchSize > (1 << 18)) { // max 256K
                _batchSize = 1 << 18;
            }
        }
    }
    uint64_t initEnd = TimeUtility::currentTime();
    incInitTime(initEnd - initBegin);
    return true;
}

bool NormalScan::doBatchScan(TablePtr &table, bool &eof) {
    vector<MatchDoc> matchDocs;
    eof = false;
    if (_scanIter != NULL && _seekCount < _limit) {
        uint32_t batchSize = _batchSize;
        if (_limit > _seekCount) {
            batchSize = std::min(_limit - _seekCount, batchSize);
        }
        matchDocs.reserve(batchSize);
        eof = _scanIter->batchSeek(batchSize, matchDocs);
        _seekCount += matchDocs.size();
        if (_seekCount >= _limit) {
            eof = true;
            uint32_t delCount = _seekCount - _limit;
            if (delCount > 0 ) {
                uint32_t reserveCount = matchDocs.size() - delCount;
                _matchDocAllocator->deallocate(matchDocs.data() + reserveCount, delCount);
                matchDocs.resize(reserveCount);
            }
        }
    } else {
        eof = true;
    }
    uint64_t afterSeek = TimeUtility::currentTime();
    incSeekTime(afterSeek - _batchScanBeginTime);
    evaluateAttribute(matchDocs, _matchDocAllocator, _attributeExpressionVec, eof);
    uint64_t afterEvaluate = TimeUtility::currentTime();
    incEvaluateTime(afterEvaluate - afterSeek);
    table = createTable(matchDocs, _matchDocAllocator, eof && _scanOnce);
    int64_t endTime = TimeUtility::currentTime();
    incOutputTime(endTime - afterEvaluate);
    if (eof && _scanIter) {
        _scanInfo.set_totalscancount(_scanInfo.totalscancount() + _scanIter->getTotalScanCount());
        if (_scanIter->isTimeout()) {
            SQL_LOG(WARN, "scan table [%s] timeout, info: [%s]", _tableName.c_str(),
                    _scanInfo.ShortDebugString().c_str());
        }
        _scanIter.reset();
    }
    return true;
}

bool NormalScan::updateScanQuery(const StreamQueryPtr &inputQuery) {
    _scanOnce = false;
    _scanIter.reset();
    uint64_t initBegin = TimeUtility::currentTime();
    if (nullptr == inputQuery) {
        return true;
    }
    const QueryPtr query = inputQuery->query;
    if (query == nullptr) {
        return true;
    }
    SQL_LOG(TRACE2, "update query [%s]", query->toString().c_str());
    CreateScanIteratorInfo createInfo = _baseCreateScanIterInfo;
    if (createInfo.query != nullptr) {
        QueryPtr andQuery(new AndQuery(""));
        andQuery->addQuery(query);
        andQuery->addQuery(createInfo.query);
        createInfo.query = andQuery;
    } else {
        createInfo.query = query;
    }
    bool emptyScan = false;
    _scanIter.reset(_scanIterCreator->createScanIterator(createInfo, emptyScan));
    if (_scanIter == NULL && !emptyScan) {
        SQL_LOG(WARN, "table [%s] create ha3 scan iterator failed", _tableName.c_str());
        return false;
    }
    uint64_t initEnd = TimeUtility::currentTime();
    incInitTime(initEnd - initBegin);
    return true;
}

bool NormalScan::initOutputColumn() {
    _attributeExpressionVec.clear();
    _attributeExpressionVec.reserve(_outputFields.size());
    map<string, ExprEntity> exprsMap;
    map<string, string> renameMap;
    bool ret = ExprUtil::parseOutputExprs(_pool, _outputExprsJson, exprsMap, renameMap);
    if (!ret) {
        SQL_LOG(WARN, "parse output exprs [%s] failed", _outputExprsJson.c_str());
        return false;
    }
    auto attributeInfos = _tableInfo->getAttributeInfos();
    map<string, pair<string, bool> > expr2Outputs;
    for (size_t i = 0; i < _outputFields.size(); i++) {
        const string &outputName = _outputFields[i];
        string outputFieldType = "";
        if (_outputFields.size() == _outputFieldsType.size())
        {
            outputFieldType = _outputFieldsType[i];
        }
        string exprJson;
        string exprStr = outputName;
        AttributeExpression* expr = NULL;
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        auto iter = exprsMap.find(outputName);
        if (iter != exprsMap.end()) {
            exprJson = iter->second.exprJson;
            exprStr = iter->second.exprStr;
        }
        AutilPoolAllocator allocator(_pool);
        SimpleDocument simpleDoc(&allocator);
        if (!exprJson.empty() && ExprUtil::parseExprsJson(exprJson, simpleDoc) && ExprUtil::isCaseOp(simpleDoc))
        {
            if (copyField(exprJson, outputName, expr2Outputs)) {
                continue;
            }
            expr = ExprUtil::CreateCaseExpression(
                _attributeExpressionCreator, outputFieldType, simpleDoc,  hasError, errorInfo, renameMap, _pool);
            if (hasError || expr == NULL)
            {
                SQL_LOG(WARN, "create case expression failed [%s]", errorInfo.c_str());
                return false;
            }
        } else {
            if (copyField(exprStr, outputName, expr2Outputs)) {
                continue;
            }
            expr = initAttributeExpr(outputName, outputFieldType, exprStr, expr2Outputs);
            if (!expr) {
                if (exprStr == outputName && attributeInfos->getAttributeInfo(outputName) == NULL) {
                    SQL_LOG(WARN, "outputName [%s] not exist in attributes, use default null string expr",
                            outputName.c_str());
                    AtomicSyntaxExpr defaultSyntaxExpr(string("null"), vt_string, STRING_VALUE);
                    expr = ExprUtil::createConstExpression(_pool, &defaultSyntaxExpr, vt_string);
                    if (expr) {
                        _attributeExpressionCreator->addNeedDeleteExpr(expr);
                    }
                }
                if (!expr)
                {
                    SQL_LOG(ERROR, "Create outputName [%s] expr [%s] failed.",
                            outputName.c_str(), exprStr.c_str());
                    return false;
                }
            }
        }
        expr->setOriginalString(outputName);
        if (!expr->allocate(_matchDocAllocator.get())) {
            SQL_LOG(WARN, "Create outputName [%s] expr [%s] failed.",
                            outputName.c_str(), exprStr.c_str());
            return false;
        }
        auto refer = expr->getReferenceBase();
        refer->setSerializeLevel(SL_ATTRIBUTE);
        _attributeExpressionVec.push_back(expr);
    }
    _matchDocAllocator->extend();
    _matchDocAllocator->extendSub();
    return true;
}

bool NormalScan::copyField(const string &expr, const string &outputName,
                           map<string, pair<string, bool> > &expr2Outputs)
{
    auto iter = expr2Outputs.find(expr);
    if (iter != expr2Outputs.end()) {
        if (!iter->second.second) {
            _copyFieldMap[outputName] = iter->second.first;
            if (appendCopyColumn(iter->second.first, outputName, _matchDocAllocator)) {
                return true;
            }
        }
    } else {
        expr2Outputs[expr] = make_pair(outputName, false);
    }
    return false;
}


suez::turing::AttributeExpression*
NormalScan::initAttributeExpr(string outputName, string outputFieldType, string exprStr,
                              map<string, pair<string, bool> >& expr2Outputs)
{
    SyntaxExpr *syntaxExpr = SyntaxParser::parseSyntax(exprStr);
    if (!syntaxExpr) {
        SQL_LOG(WARN, "parse syntax [%s] failed", exprStr.c_str());
        return NULL;
    }

    AttributeExpression *expr = NULL;
    if (syntaxExpr->getSyntaxExprType() == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
        auto iter2 = expr2Outputs.find(exprStr);
        if (iter2 != expr2Outputs.end()) {
            iter2->second.second = true;
        }
        AtomicSyntaxExpr *atomicExpr = dynamic_cast<AtomicSyntaxExpr*>(syntaxExpr);
        if (atomicExpr) {
            ExprResultType resultType = vt_unknown;
            const AttributeInfos *attrInfos = _tableInfo->getAttributeInfos();
            if (attrInfos) {
                const AttributeInfo *attrInfo = attrInfos->getAttributeInfo(outputName.c_str());
                if (attrInfo) {
                    FieldType fieldType = attrInfo->getFieldType();
                    resultType = TypeTransformer::transform(fieldType);
                }
            }
            if (resultType == vt_unknown) {
                if (outputFieldType != "") {
                    resultType = ExprUtil::transSqlTypeToVariableType(outputFieldType);
                } else {
                    AtomicSyntaxExprType type = atomicExpr->getAtomicSyntaxExprType();
                    if (type == FLOAT_VALUE) {
                        resultType = vt_float;
                    } else if (type == INTEGER_VALUE) {
                        resultType = vt_int32;
                    } else if (type == STRING_VALUE) {
                        resultType = vt_string;
                    }
                }
            }
            expr = ExprUtil::createConstExpression(_pool, atomicExpr, resultType);
            if (expr) {
                SQL_LOG(TRACE2, "create const expression [%s], result type [%d]",
                        exprStr.c_str(), resultType)
                    _attributeExpressionCreator->addNeedDeleteExpr(expr);
            }
        }
    }
    if (!expr) {
        expr = _attributeExpressionCreator->createAttributeExpression(syntaxExpr, true);
    }
    DELETE_AND_SET_NULL(syntaxExpr);
    return expr;
}

bool NormalScan::appendCopyColumn(const std::string &srcField, const std::string &destField,
                                  MatchDocAllocatorPtr &matchDocAllocator)
{
    auto srcRef = matchDocAllocator->findReferenceWithoutType(srcField);
    if (!srcRef) {
        SQL_LOG(WARN, "not found column [%s]", srcField.c_str());
        return false;
    }
    auto destRef = matchDocAllocator->cloneReference(
            srcRef, destField, matchDocAllocator->getDefaultGroupName());
    if (!destRef) {
        SQL_LOG(WARN, "clone column from [%s] to [%s] failed",
                srcField.c_str(), destField.c_str());
        return false;
    }
    return true;
}

void NormalScan::copyColumns(const map<string, string> &copyFieldMap,
                             const vector<MatchDoc> &matchDocVec,
                             MatchDocAllocatorPtr &matchDocAllocator)
{
    for (auto iter : copyFieldMap) {
        auto srcRef = matchDocAllocator->findReferenceWithoutType(iter.second);
        if (!srcRef) {
            continue;
        }
        auto destRef = matchDocAllocator->findReferenceWithoutType(iter.first);
        if (!destRef) {
            continue;
        }
        for (auto matchDoc : matchDocVec) {
            destRef->cloneConstruct(matchDoc, matchDoc, srcRef);
        }
    }
}

void NormalScan::evaluateAttribute(vector<MatchDoc> &matchDocVec,
                                   MatchDocAllocatorPtr &matchDocAllocator,
                                   vector<AttributeExpression *> &attributeExpressionVec,
                                   bool eof)
{
    ExpressionEvaluator<vector<AttributeExpression *> > evaluator(
            attributeExpressionVec,
            matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(matchDocVec.data(), matchDocVec.size());
    copyColumns(_copyFieldMap, matchDocVec, matchDocAllocator);
}

TablePtr NormalScan::createTable(vector<MatchDoc> &matchDocVec,
                                 const MatchDocAllocatorPtr &matchDocAllocator,
                                 bool reuseMatchDocAllocator)
{
    TablePtr table;
    MatchDocAllocatorPtr outputAllocator = matchDocAllocator;
    vector<MatchDoc> copyMatchDocs;
    if (!reuseMatchDocAllocator) {
        auto naviPool = _memoryPoolResource->getPool();
        if (_tableMeta.empty()) {
            DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, _pool);
            matchDocAllocator->setSortRefFlag(false);
            matchDocAllocator->serializeMeta(dataBuffer, 0);
            _tableMeta.append(dataBuffer.getData(), dataBuffer.getDataLen());
        }
        DataBuffer dataBuffer((void*)_tableMeta.c_str(), _tableMeta.size());
        outputAllocator.reset(new MatchDocAllocator(naviPool));
        outputAllocator->deserializeMeta(dataBuffer, NULL);
        if (!matchDocAllocator->swapDocStorage(*outputAllocator, copyMatchDocs, matchDocVec)) {
            DataBuffer buffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, _pool);
            matchDocAllocator->serialize(buffer, matchDocVec, SL_ATTRIBUTE);
            outputAllocator.reset(new MatchDocAllocator(naviPool));
            outputAllocator->deserialize(buffer, copyMatchDocs);
            matchDocAllocator->deallocate(matchDocVec.data(), matchDocVec.size());
        }
    } else {
        copyMatchDocs.swap(matchDocVec);
    }

    if (_useSub) {
        flattenSub(outputAllocator, copyMatchDocs, table);
        CalcTable calcTable(_pool, _memoryPoolResource, _outputFields, _outputFieldsType,
                            nullptr, nullptr, {}, nullptr);
        if (!calcTable.projectTable(table)) {
            SQL_LOG(WARN, "project table [%s] failed.", TableUtil::toString(table, 5).c_str());
        }
        return table;
    } else {
        table.reset(new Table(copyMatchDocs, outputAllocator));
        return table;
    }
}

void NormalScan::flattenSub(MatchDocAllocatorPtr &outputAllocator,
                            const vector<MatchDoc> &copyMatchDocs,
                            TablePtr &table)
{
    vector<MatchDoc> outputMatchDocs;
    auto accessor = outputAllocator->getSubDocAccessor();
    std::function<void(MatchDoc)> processNothing = [&outputMatchDocs](MatchDoc newDoc) {
        outputMatchDocs.push_back(newDoc);
    };
    auto firstSubRef = outputAllocator->getFirstSubDocRef();
    vector<MatchDoc> needConstructSubDocs;
    for (MatchDoc doc : copyMatchDocs) {
        MatchDoc &first = firstSubRef->getReference(doc);
        if (first != INVALID_MATCHDOC) {
            accessor->foreachFlatten(doc, outputAllocator.get(), processNothing);
        } else {
            if (_nestTableJoinType == INNER_JOIN) {
                continue;
            }
            // alloc empty sub doc
            MatchDoc subDoc = outputAllocator->allocateSub(doc);
            needConstructSubDocs.emplace_back(subDoc);
            outputMatchDocs.push_back(doc);
        }
    }
    if (!needConstructSubDocs.empty()) {
        ReferenceVector subRefs = outputAllocator->getAllSubNeedSerializeReferences(0);
        for (auto &ref : subRefs) {
            for (auto subDoc : needConstructSubDocs) {
                ref->construct(subDoc);
            }
        }
    }
    table.reset(new Table(outputMatchDocs, outputAllocator));
}

END_HA3_NAMESPACE(sql);
