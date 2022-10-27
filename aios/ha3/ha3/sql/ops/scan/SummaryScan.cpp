#include <ha3/sql/ops/scan/SummaryScan.h>
#include <ha3/sql/ops/scan/PrimaryKeyScanConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/util/ValueTypeSwitch.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/util/StringConvertor.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <suez/turing/expression/util/TypeTransformer.h>
#include <autil/HashFuncFactory.h>
#include <autil/ConstStringUtil.h>
#include <indexlib/util/hash_string.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <ha3/common/QueryTermVisitor.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, SummaryScan);

SummaryScan::SummaryScan(bool requirePk)
    : _requirePk(requirePk)
{
    _range.set_from(0);
    _range.set_to(MAX_PARTITION_RANGE);
}

SummaryScan::~SummaryScan() {
}

bool SummaryScan::init(const ScanInitParam &param) {
    uint64_t initBegin = TimeUtility::currentTime();
    if (!ScanBase::init(param)) {
        SQL_LOG(ERROR, "init scan base failed");
        return false;
    }
    SqlBizResource *bizResource = param.bizResource;
    SqlQueryResource *queryResource = param.queryResource;
    initSummary();
    initExtraSummary(bizResource, queryResource);

    auto sqlSessionResource =
        bizResource->getObject<turing::SqlSessionResource>("SqlSessionResource");
    if (sqlSessionResource) {
        _range = sqlSessionResource->range;
    }
    // init pk query
    std::vector<std::string> rawPks;
    bool needFilter = true;
    if (!parseQuery(rawPks, needFilter)) {
        SQL_LOG(ERROR, "parse query failed");
        return false;
    }
    StringUtil::toUpperCase(_hashFunc);
    _hashFuncPtr = HashFuncFactory::createHashFunc(_hashFunc);
    if (_hashFuncPtr == nullptr) {
        SQL_LOG(ERROR, "create hash func [%s] failed", _hashFunc.c_str());
        return false;
    }

    std::vector<primarykey_t> pks;
    SQL_LOG(TRACE2, "before filter docid by range, size: %lu", rawPks.size());
    filterDocIdByRange(rawPks, _range, pks);
    SQL_LOG(TRACE2, "after filter docid by range, size: %lu", pks.size());
    if (!convertPK2DocId(pks)) {
        SQL_LOG(ERROR, "convert pk to docid failed");
        return false;
    }
    SQL_LOG(TRACE2, "after convert PK to DocId, size: %lu", _docIds.size());
    if (!prepareSummaryReader()) {
        SQL_LOG(ERROR, "prepare summary reader failed");
        return false;
    }
    // sort usedFields
    if (_usedFields != _outputFields) {
        adjustUsedFields();
    }
    // prepare attr && _table declare column
    if (!prepareFields()) {
        SQL_LOG(ERROR, "prepare fields failed");
        return false;
    }
    // init calc
    _calcTable.reset(new CalcTable(_pool, _memoryPoolResource,
                                   _outputFields, _outputFieldsType,
                                   queryResource->getTracer(),
                                   queryResource->getSuezCavaAllocator(),
                                   bizResource->getCavaPluginManager(),
                                   bizResource->getFunctionInterfaceCreator().get()));
    if (!_calcTable->init(_outputExprsJson, _conditionJson)) {
        SQL_LOG(ERROR, "calc table init failed");
        return false;
    }
    // set if need filter
    _calcTable->setFilterFlag(needFilter);
    uint64_t initEnd = TimeUtility::currentTime();
    incInitTime(initEnd - initBegin);
    return true;
}

void SummaryScan::initSummary() {
    _indexPartitionReaderWrappers.emplace_back(_indexPartitionReaderWrapper);
    _attributeExpressionCreators.emplace_back(_attributeExpressionCreator);
}

void SummaryScan::initExtraSummary(SqlBizResource *bizResource,
                                   SqlQueryResource *queryResource)
{
    string extraTableName = _tableName + "_extra";
    auto tableInfo = bizResource->getTableInfo(extraTableName);
    if (!tableInfo) {
        SQL_LOG(TRACE1, "table info [%s] not found", extraTableName.c_str());
        return;
    }

    auto mainIndexPartReader =
        queryResource->getPartitionReaderSnapshot()->GetIndexPartitionReader(extraTableName);
    if (!mainIndexPartReader) {
        SQL_LOG(TRACE1, "table [%s] not found", extraTableName.c_str());
        return;
    }
    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper(
            new IndexPartitionReaderWrapper(NULL, NULL, {mainIndexPartReader}));
    _indexPartitionReaderWrappers.emplace_back(indexPartitionReaderWrapper);
    AttributeExpressionCreatorPtr attributeExpressionCreator(new AttributeExpressionCreator(
                    _pool, _matchDocAllocator.get(),
                    extraTableName, queryResource->getPartitionReaderSnapshot(),
                    tableInfo, {},
                    bizResource->getFunctionInterfaceCreator().get(),
                    bizResource->getCavaPluginManager(), _functionProvider.get(),
                    tableInfo->getSubTableName()));
    _attributeExpressionCreators.emplace_back(attributeExpressionCreator);
}

void SummaryScan::adjustUsedFields() {
    if (_usedFields.size() == _outputFields.size()) {
        std::unordered_set<std::string> outputFieldsSet(_outputFields.begin(), _outputFields.end());
        bool needSort = true;
        for (size_t i = 0; i < _usedFields.size(); ++i) {
            if (outputFieldsSet.count(_usedFields[i]) == 0) {
                needSort = false;
                break;
            }
        }
        if (needSort) {
            _usedFields = _outputFields;
        }
    }
}

bool SummaryScan::doBatchScan(TablePtr &table, bool &eof) {
    _scanInfo.set_totalscancount(_scanInfo.totalscancount() + _docIds.size());
    auto matchDocs = _matchDocAllocator->batchAllocate(_docIds);
    if (!fillSummary(matchDocs)) {
        SQL_LOG(ERROR, "fill summary failed");
        return false;
    }
    if (!fillAttributes(matchDocs)) {
        SQL_LOG(ERROR, "fill attribute failed");
        return false;
    }
    uint64_t afterSeek = TimeUtility::currentTime();
    incSeekTime(afterSeek - _batchScanBeginTime);
    table = createTable(matchDocs, _matchDocAllocator, _scanOnce);
    int64_t outputTime = TimeUtility::currentTime();
    incOutputTime(outputTime - afterSeek);
    if (!_calcTable->calcTable(table)) {
        SQL_LOG(ERROR, "calc table failed");
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    _seekCount += table->getRowCount();
    incEvaluateTime(afterEvaluate - outputTime);
    if (_limit < _seekCount) {
        table->clearBackRows(min(_seekCount - _limit, (uint32_t)table->getRowCount()));
    }
    int64_t endTime = TimeUtility::currentTime();
    incOutputTime(endTime - afterEvaluate);
    eof = true;
    return true;
}

bool SummaryScan::updateScanQuery(const StreamQueryPtr &inputQuery) {
    _calcTable->setFilterFlag(true);
    _scanOnce = false;
    if (nullptr != inputQuery) {
        const common::QueryPtr query = inputQuery->query;
        if (query != nullptr) {
            if (!genDocIdFromQuery(query)) {
                SQL_LOG(WARN, "generate docid failed, query [%s]", query->toString().c_str());
                return false;
            }
        }
    }
    return true;
}

bool SummaryScan::genDocIdFromQuery(const common::QueryPtr &query) {
    vector<string> rawPks;
    if (!genRawDocIdFromQuery(query, rawPks)) {
        return false;
    }
    std::vector<primarykey_t> pks;
    filterDocIdByRange(rawPks, _range, pks);
    if (!convertPK2DocId(pks)) {
        SQL_LOG(ERROR, "convert pk to docid failed");
        return false;
    }
    return true;
}

bool SummaryScan::genRawDocIdFromQuery(const common::QueryPtr &query, vector<string> &rawPks) {
    if (query == nullptr) {
        return true;
    }
    const suez::turing::IndexInfo *pkIndexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    if (pkIndexInfo == nullptr) {
        return false;
    }
    string indexName = pkIndexInfo->getIndexName();
    QueryTermVisitor termVisitor;
    query->accept(&termVisitor);
    const TermVector &termVec = termVisitor.getTermVector();
    rawPks.reserve(termVec.size());
    for (auto &term : termVec) {
        if (indexName == term.getIndexName()) {
            rawPks.emplace_back(term.getWord());
        }
    }
    return true;
}

bool SummaryScan::parseQuery(std::vector<std::string> &pks, bool &needFilter) {
    ConditionParser parser(_pool);
    ConditionPtr condition;
    if (!parser.parseCondition(_conditionJson, condition)) {
        SQL_LOG(ERROR, "table name [%s], parse condition [%s] failed",
                        _tableName.c_str(), _conditionJson.c_str());
        return false;
    }
    if (condition == nullptr) {
        return !_requirePk;
    }
    if (!_tableInfo) {
        SQL_LOG(ERROR, "table name [%s], talble info is null", _tableName.c_str());
        return false;
    }
    auto indexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    if (!indexInfo) {
        SQL_LOG(ERROR, "table name [%s], pk index info is null", _tableName.c_str());
        return false;
    }
    SummaryScanConditionVisitor visitor(indexInfo->fieldName, indexInfo->indexName);
    CHECK_VISITOR_ERROR(visitor);
    condition->accept(&visitor);
    CHECK_VISITOR_ERROR(visitor);
    if (!visitor.stealHasQuery()) {
        if (_requirePk) {
            string errorMsg;
            if (condition) {
                errorMsg = "condition: " + condition->toString();
            } else {
                errorMsg = "condition json: " + _conditionJson;
            }
            SQL_LOG(ERROR, "summary table name [%s] need pk query, %s",
                    _tableName.c_str(), errorMsg.c_str());
        }
        return !_requirePk;
    }
    pks = visitor.getRawKeyVec();
    needFilter = visitor.needFilter();
    return true;
}

primarykey_t SummaryScan::calculatePrimaryKey(const string &rawPk) {
    const suez::turing::IndexInfo *pkIndexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    assert(pkIndexInfo != NULL);

    IndexType pkIndexType = pkIndexInfo->getIndexType();
    IE_NAMESPACE(util)::HashString hashFunc;
    primarykey_t hashedPrimaryKey = 0;
    if (pkIndexType == it_primarykey64) {
        hashFunc.Hash(hashedPrimaryKey.value[1], rawPk.c_str(), rawPk.size());
    } else {
        hashFunc.Hash(hashedPrimaryKey, rawPk.c_str(), rawPk.size());
    }
    return hashedPrimaryKey;
}

bool SummaryScan::prepareFields() {
    auto attributeInfos = _tableInfo->getAttributeInfos();
    if (attributeInfos == nullptr) {
        SQL_LOG(ERROR, "get attribute infos failed");
        return false;
    }
    const SummaryInfo *summaryInfo = _tableInfo->getSummaryInfo();
    if (summaryInfo == nullptr) {
        SQL_LOG(ERROR, "get summary info failed");
        return false;
    }
    if (_usedFields.empty()) {
        SQL_LOG(ERROR, "used fields can not be empty");
        return false;
    }
    _tableAttributes.resize(_attributeExpressionCreators.size());
    std::vector<MatchDoc> emptyMatchDocs;
    const FieldInfos *fieldInfos = _tableInfo->getFieldInfos();
    if (fieldInfos == nullptr) {
        SQL_LOG(ERROR, "get field infos failed");
        return false;
    }
    for (const auto &usedField : _usedFields) {
        auto attributeInfo = attributeInfos->getAttributeInfo(usedField);
        if (attributeInfo == nullptr) {
            if (!summaryInfo->exist(usedField)) {
                SQL_LOG(WARN, "field [%s] not exist in schema, use default null string expr",
                        usedField.c_str());
                for (size_t i = 0; i < _attributeExpressionCreators.size(); ++i) {
                    AtomicSyntaxExpr defaultSyntaxExpr(
                            string("null"), vt_string, STRING_VALUE);
                    auto expr = ExprUtil::createConstExpression(
                            _pool, &defaultSyntaxExpr, vt_string);
                    expr->setOriginalString(usedField);
                    _attributeExpressionCreators[i]->addNeedDeleteExpr(expr);
                    if (!expr->allocate(_matchDocAllocator.get())) {
                        SQL_LOG(WARN, "allocate null string expr failed");
                        return false;
                    }
                    auto ref = expr->getReferenceBase();
                    ref->setSerializeLevel(SL_ATTRIBUTE);
                    _tableAttributes[i].push_back(expr);
                }
                continue;
            }
            auto fieldInfo = fieldInfos->getFieldInfo(usedField.data());
            if (fieldInfo == nullptr) {
                SQL_LOG(ERROR, "get field info[%s] failed", usedField.c_str());
                return false;
            }
            matchdoc::BuiltinType bt = TypeTransformer::transform(fieldInfo->fieldType);
            bool isMulti = fieldInfo->isMultiValue;
            auto func = [&](auto a) {
                typedef typename decltype(a)::value_type T;
                auto ref = _matchDocAllocator->declareWithConstructFlagDefaultGroup<T>(
                        usedField, matchdoc::ConstructTypeTraits<T>::NeedConstruct(),
                        SL_ATTRIBUTE);
                return ref != nullptr;
            };
            if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
                SQL_LOG(ERROR, "declare column[%s] failed", usedField.c_str());
                return false;
            }
        } else {
            for (size_t i = 0; i < _attributeExpressionCreators.size(); ++i) {
                auto attrExpr =
                    _attributeExpressionCreators[i]->createAttributeExpression(usedField);
                if (attrExpr == nullptr) {
                    SQL_LOG(ERROR, "create attr expr [%s] failed", usedField.c_str());
                    return false;
                }
                if (!attrExpr->allocate(_matchDocAllocator.get())) {
                    SQL_LOG(WARN, "allocate attr expr [%s] failed", usedField.c_str());
                    return false;
                }
                auto ref = attrExpr->getReferenceBase();
                ref->setSerializeLevel(SL_ATTRIBUTE);
                _tableAttributes[i].push_back(attrExpr);
            }
        }
    }
    _matchDocAllocator->extend();
    return true;
}

bool SummaryScan::convertPK2DocId(const std::vector<primarykey_t> &pks)
{
    std::vector<IndexReader *> primaryKeyReaders;
    for (auto indexPartitionReaderWrapper : _indexPartitionReaderWrappers) {
        IndexReader *primaryKeyReader =
            indexPartitionReaderWrapper->getReader()->GetPrimaryKeyReader().get();
        if (!primaryKeyReader) {
            SQL_LOG(ERROR, "can not find primary key index");
            return false;
        }
        primaryKeyReaders.emplace_back(primaryKeyReader);
    }
    assert(primaryKeyReaders.size() > 0);
    if (primaryKeyReaders[0]->GetIndexType() == it_primarykey64) {
        return convertPK2DocId<uint64_t>(pks, primaryKeyReaders);
    } else {
        return convertPK2DocId<uint128_t>(pks, primaryKeyReaders);
    }
}

bool SummaryScan::prepareSummaryReader() {
    for (auto indexPartitionReaderWrapper : _indexPartitionReaderWrappers) {
        auto indexPartReader = indexPartitionReaderWrapper->getReader();
        auto summaryReader = indexPartReader->GetSummaryReader();
        if (!summaryReader) {
            SQL_LOG(ERROR, "can not find summary reader");
            return false;
        }
        _summaryReaders.emplace_back(summaryReader);
    }
    return true;
}

void SummaryScan::filterDocIdByRange(const std::vector<std::string> &rawPks,
                                     const proto::Range &range,
                                     std::vector<primarykey_t> &pks)
{
    pks.clear();
    const suez::turing::IndexInfo *pkIndexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    assert(pkIndexInfo != NULL);
    string hashField = _hashFields.size() > 0 ? _hashFields[0] : "";
    for (const auto &rawPk: rawPks) {
        uint32_t hashId = _hashFuncPtr->getHashId(rawPk);
        if (hashId % _parallelNum == _parallelIndex) {
            if (hashField != pkIndexInfo->fieldName) {
                pks.emplace_back(calculatePrimaryKey(rawPk));
            } else if (hashId >= range.from() && hashId <= range.to()) {
                pks.emplace_back(calculatePrimaryKey(rawPk));
            }
        }
    }
}

template <typename T>
bool SummaryScan::fillSummaryDocs(const Reference<T> *ref,
                                  const SearchSummaryDocVecType &summaryDocs,
                                  const vector<matchdoc::MatchDoc> &matchDocs,
                                  int32_t summaryFieldId)
{
    if (ref == nullptr) {
        SQL_LOG(ERROR, "reference is empty.");
        return false;
    }
    if (summaryDocs.size() != matchDocs.size()) {
        SQL_LOG(ERROR, "size not equal left [%ld], right [%ld].",
                summaryDocs.size(), matchDocs.size());
        return false;
    }
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        const ConstString *value = summaryDocs[i].GetFieldValue(summaryFieldId);
        if (value == nullptr) {
            SQL_LOG(ERROR, "summary get field id [%d] value failed", summaryFieldId);
            return false;
        }
        T val;
        if (value->size() == 0) {
            // default construct
            InitializeIfNeeded<T>()(val);
        } else {
            if (!StringConvertor::fromString(value, val, _pool)) {
                SQL_LOG(TRACE2, "summary value [%s], size [%ld], type [%s] convert failed",
                        value->toString().c_str(), value->size(), typeid(val).name());
                InitializeIfNeeded<T>()(val);
            }
        }
        ref->set(matchDocs[i], val);
    }
    return true;
}

bool SummaryScan::getSummaryDocs(const SummaryInfo *summaryInfo,
                                 const vector<matchdoc::MatchDoc> &matchDocs,
                                 SearchSummaryDocVecType &summaryDocs)
{
    summaryDocs.reserve(matchDocs.size());
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        summaryDocs.emplace_back(_pool, summaryInfo->getFieldCount());
        if (!_summaryReaders[_tableIdx[i]]->GetDocument(
                        matchDocs[i].getDocId(), &summaryDocs[i]))
        {
            SQL_LOG(ERROR, "get doc [%d] from summary failed", matchDocs[i].getDocId());
            return false;
        }
    }
    return true;
}

bool SummaryScan::fillSummary(const vector<matchdoc::MatchDoc> &matchDocs) {
    auto summaryInfo = _tableInfo->getSummaryInfo();
    if (summaryInfo == nullptr) {
        SQL_LOG(ERROR, "get summary info failed");
        return false;
    }
    SearchSummaryDocVecType summaryDocs;
    if (!getSummaryDocs(summaryInfo, matchDocs, summaryDocs)) {
        return false;
    }
    for (size_t i = 0; i < summaryInfo->getFieldCount(); ++i) {
        const std::string &fieldName = summaryInfo->getFieldName(i);
        ReferenceBase *refBase = _matchDocAllocator->findReferenceWithoutType(fieldName);
        if (refBase == nullptr) {
            continue;
        }
        auto vt = refBase->getValueType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            Reference<T> *ref = dynamic_cast<Reference<T> *>(refBase);
            return this->fillSummaryDocs<T>(ref, summaryDocs, matchDocs, i);
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            SQL_LOG(ERROR, "fill summary docs column[%s] failed", fieldName.c_str());
            return false;
        }
    }
    return true;
}

bool SummaryScan::fillAttributes(const std::vector<matchdoc::MatchDoc> &matchDocs) {
    vector<vector<matchdoc::MatchDoc>> tableMatchDocs(_tableAttributes.size());
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        tableMatchDocs[_tableIdx[i]].emplace_back(matchDocs[i]);
    }
    for (size_t i = 0; i < tableMatchDocs.size(); ++i) {
        for (auto &attrExpr : _tableAttributes[i]) {
            if (!attrExpr->batchEvaluate(tableMatchDocs[i].data(), tableMatchDocs[i].size())) {
                SQL_LOG(ERROR, "batch evaluate failed, attr expr [%s]",
                        attrExpr->getOriginalString().c_str());
                return false;
            }
        }
    }
    return true;
}

TablePtr SummaryScan::createTable(vector<MatchDoc> &matchDocVec,
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
    table.reset(new Table(copyMatchDocs, outputAllocator));
    return table;
}

END_HA3_NAMESPACE(sql);
