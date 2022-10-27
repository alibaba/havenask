#include <ha3_sdk/testlib/summary/SummaryTestHelper.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <indexlib/index/normal/summary/summary_reader.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryTestHelper);

SummaryTestHelper::SummaryTestHelper(const std::string &testPath) {
    _testPath = testPath;
    _param = nullptr;
    _pool = nullptr;
    _provider = nullptr;
    _queryInfo = nullptr;
    _attrExprCreator = nullptr;
    _hitSummarySchema = nullptr;
    _tracer = nullptr;
    _ha3CavaAllocator = nullptr;
    _resource = nullptr;
}

SummaryTestHelper::~SummaryTestHelper() {
    POOL_DELETE_CLASS(_hitSummarySchema);
    for (size_t i = 0; i < _fieldSummaryConfigVec.size(); i++) {
        POOL_DELETE_CLASS(_fieldSummaryConfigVec[i]);
    }
    POOL_DELETE_CLASS(_provider);
    POOL_DELETE_CLASS(_queryInfo);
    POOL_DELETE_CLASS(_attrExprCreator);
    POOL_DELETE_CLASS(_tracer);
    POOL_DELETE_CLASS(_resource);
    DELETE_AND_SET_NULL(_pool);
    DELETE_AND_SET_NULL(_ha3CavaAllocator);
}

bool SummaryTestHelper::init(SummaryTestParam *param) {
    _param = param;

    _pool = new Pool;

    _invertedTableParam.addFakeIndexData(param->fakeIndex.attributes,
            param->fakeIndex.summarys);
    auto testPath = _testPath + "_" + autil::StringUtil::toString(autil::TimeUtility::currentTime());
    _indexWrap.reset(new IndexWrap(testPath));
    _indexWrap->addInvertedTable(&_invertedTableParam);
    _indexWrap->buildIndex();
    _snapshotPtr = _indexWrap->CreateSnapshot();
    _tableInfoPtr = _indexWrap->createTableInfo(_invertedTableParam.tableName);
    if (_param->requestStr.empty()) {
        _param->requestStr = "config=cluster:test";
    }
    _requestPtr = RequestCreator::prepareRequest(_param->requestStr);

    _ha3CavaAllocator = new suez::turing::SuezCavaAllocator(_pool, 1024);
    std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
    _resource = POOL_NEW_CLASS(_pool, SearchCommonResource, _pool, _tableInfoPtr,
                               nullptr, nullptr, _tracer, nullptr, suez::turing::CavaPluginManagerPtr(),
                               _requestPtr.get(), _ha3CavaAllocator, cavaJitModules);
    _mdAllocator = _resource->matchDocAllocator.get();

    if (!createHitSummarySchema(_param->summarySchemaStr)) {
        return false;
    }
    if (!createFieldSummaryConfig(_param->fieldSummaryConfigStr)) {
        return false;
    }
    if (!createSummaryQueryInfo()) {
        return false;
    }
    if (!createAttrExprCreator()) {
        return false;
    }
    if (!createTracer()) {
        return false;
    }

    return createProvider();
}

bool SummaryTestHelper::createTracer() {
    Tracer *tracer = POOL_NEW_CLASS(_pool, Tracer);
    if (tracer == nullptr)
        return false;
    _tracer = tracer;
    _tracer->setLevel(ISEARCH_TRACE_TRACE3);
    return true;
}

bool SummaryTestHelper::createAttrExprCreator() {
    VirtualAttributes virtualAttributes;
    AttributeExpressionCreator *creator =
        POOL_NEW_CLASS(_pool, AttributeExpressionCreator, _pool, _mdAllocator,
                       _invertedTableParam.tableName, _snapshotPtr.get(), _tableInfoPtr,
                       virtualAttributes, nullptr, suez::turing::CavaPluginManagerPtr(), nullptr);
    _attrExprCreator = creator;
    return _attrExprCreator != nullptr;
}

bool SummaryTestHelper::createSummaryQueryInfo() {
    _queryInfo = POOL_NEW_CLASS(_pool, SummaryQueryInfo);
    if (_queryInfo == nullptr) {
        return false;
    }
    _queryInfo->queryString = _param->queryStr;
    vector<string> termVec;
    StringUtil::fromString(_param->termStr, termVec, ",");
    for (size_t i = 0; i < termVec.size(); i++) {
        Term t;
        t.setWord(termVec[i].c_str());
        t.setIndexName("default");
        _queryInfo->terms.push_back(t);
    }
    return true;
}

bool SummaryTestHelper::createProvider() {
    _provider = POOL_NEW_CLASS(_pool, SummaryExtractorProvider, _queryInfo,
                               &_fieldSummaryConfigVec, _requestPtr.get(),
                               _attrExprCreator, _hitSummarySchema,
                               *_resource);
    return _provider != nullptr;
}

bool SummaryTestHelper::createHitSummarySchema(const string &schemaStr)
{
    vector<vector<string> > strVecVec;
    StringUtil::fromString(schemaStr, strVecVec, ":", ";");
    _hitSummarySchema = POOL_NEW_CLASS(_pool, HitSummarySchema);
    for (size_t i = 0; i < strVecVec.size(); i++) {
        const vector<string> &oneField = strVecVec[i];
        assert(oneField.size() >= 2);
        const string &fieldName = oneField[0];
        FieldType fieldType = strToFieldType(oneField[1]);
        bool isMulti = false;
        if (oneField.size() > 2) {
            isMulti = oneField[2] == "true" ? true :false;
        }
        summaryfieldid_t fieldId = _hitSummarySchema->declareSummaryField(
                fieldName, fieldType, isMulti);
        if (fieldId == INVALID_SUMMARYFIELDID) {
            return false;
        }
    }
    return true;
}

bool SummaryTestHelper::createFieldSummaryConfig(const string &configStr) {
    vector<string> strVec;
    StringUtil::fromString(configStr, strVec, ";");
    for (size_t i = 0; i < strVec.size(); i++) {
        const string &oneFieldConfigStr = strVec[i];
        vector<string> fieldConfigVec;
        StringUtil::fromString(oneFieldConfigStr, fieldConfigVec, ":");
        assert(fieldConfigVec.size() == 2);
        const string &fieldName = fieldConfigVec[0];
        const string &fieldConfigStr = fieldConfigVec[1];
        summaryfieldid_t fieldId = _hitSummarySchema->getSummaryFieldId(fieldName);
        assert(fieldId != INVALID_SUMMARYFIELDID);
        vector<string> tmpVec;
        StringUtil::fromString(fieldConfigStr, tmpVec, ",");
        assert(tmpVec.size() == 5);
        if (fieldId >= (summaryfieldid_t)_fieldSummaryConfigVec.size()) {
            _fieldSummaryConfigVec.resize(fieldId);
        }
        _fieldSummaryConfigVec[fieldId] = POOL_NEW_CLASS(_pool, FieldSummaryConfig);
        _fieldSummaryConfigVec[fieldId]->_maxSnippet =
            StringUtil::fromString<uint32_t>(tmpVec[0]);
        _fieldSummaryConfigVec[fieldId]->_maxSummaryLength =
            StringUtil::fromString<uint32_t>(tmpVec[1]);
        _fieldSummaryConfigVec[fieldId]->_highlightPrefix = tmpVec[2];
        _fieldSummaryConfigVec[fieldId]->_highlightSuffix = tmpVec[3];
        _fieldSummaryConfigVec[fieldId]->_snippetDelimiter = tmpVec[4];
    }
    return true;
}

FieldType SummaryTestHelper::strToFieldType(const string &fieldStr) {
    if (fieldStr == "int8_t") {
        return ft_int8;
    } else if (fieldStr == "uint8_t") {
        return ft_uint8;
    } else if (fieldStr == "int16_t") {
        return ft_uint8;
    } else if (fieldStr == "uint16_t") {
        return ft_uint8;
    } else if (fieldStr == "int32_t") {
        return ft_uint8;
    } else if (fieldStr == "uint32_t") {
        return ft_uint8;
    } else if (fieldStr == "int64_t") {
        return ft_uint8;
    } else if (fieldStr == "uint64_t") {
        return ft_uint8;
    } else if (fieldStr == "string") {
        return ft_string;
    } else if (fieldStr == "text") {
        return ft_text;
    } else if (fieldStr == "float") {
        return ft_float;
    } else if (fieldStr == "double") {
        return ft_double;
    }
    return ft_unknown;
}

bool SummaryTestHelper::beginRequest(SummaryExtractor *extractor) {
    return extractor->beginRequest(_provider);
}

bool SummaryTestHelper::fillSummary(const vector<matchdoc::MatchDoc> &matchDocs,
                                    const vector<SummaryHit*> &hits)
{
    // fill summary first
    SummaryReaderPtr summaryReader = _snapshotPtr->GetIndexPartitionReader(
            _invertedTableParam.tableName)->GetSummaryReader();
    for (size_t i = 0; i < matchDocs.size(); i++) {
        docid_t docId = matchDocs[i].getDocId();
        IE_NAMESPACE(document)::SearchSummaryDocument *summaryDoc =
            hits[i]->getSummaryDocument();
        bool ret = summaryReader->GetDocument(docId, summaryDoc);
        if (!ret) {
            return false;
        }
    }

    // then, fill attributes to summary
    typedef map<summaryfieldid_t, AttributeExpression*> AttributeMap;
    const AttributeMap& attributes = _provider->getFilledAttributes();
    for (AttributeMap::const_iterator it = attributes.begin();
         it != attributes.end(); it++)
    {
        summaryfieldid_t summaryFieldId = it->first;
        AttributeExpression *attrExpr = it->second;
        for (size_t i = 0; i < matchDocs.size(); i++) {
            std::cout<<matchDocs[i].getDocId()<<std::endl;
            attrExpr->evaluate(matchDocs[i]);
            auto reference = attrExpr->getReferenceBase();
            string str = reference->toString(matchDocs[i]);
            hits[i]->setFieldValue(summaryFieldId, str.data(), str.size());
        }
    }
    return true;
}

bool SummaryTestHelper::extractSummarys(SummaryExtractor *extractor) {
#define FREE() {\
        MatchDocCreator::deallocateMatchDocs(matchDocs, _mdAllocator);\
        for (size_t i = 0; i < hits.size(); i++) {                    \
            POOL_DELETE_CLASS(hits[i]);                               \
        }                                                             \
    }
    // 1. create matchDocs and summaryHits
    vector<matchdoc::MatchDoc> matchDocs;
    MatchDocCreator::allocateMatchDocs(_param->docIdStr, matchDocs, _mdAllocator);
    vector<SummaryHit*> hits;
    for (size_t i = 0; i < matchDocs.size(); i++) {
        SummaryHit *hit = POOL_NEW_CLASS(_pool, SummaryHit, _hitSummarySchema, _pool);
        hits.push_back(hit);
    }

    // 2. fill summary
    if (!fillSummary(matchDocs, hits)) {
        FREE();
        return false;
    }

    // 3. extract summary
    for (size_t i = 0; i < hits.size(); i++) {
        extractor->extractSummary(*hits[i]);
    }

    // 4. check result
    bool ret = checkResult(hits);

    // 5. free memory
    FREE();
#undef FREE
    return ret;
}

bool SummaryTestHelper::checkResult(const vector<SummaryHit*> &hits) {
    vector<vector<string> >  expectedSummaryDocs;
    StringUtil::fromString(_param->expectedSummaryStr,
                           expectedSummaryDocs, ",", ";");
    if (expectedSummaryDocs.size() != hits.size()) {
        return false;
    }
    for (size_t i = 0; i < hits.size(); i++) {
        SummaryHit *hit = hits[i];
        IE_NAMESPACE(document)::SearchSummaryDocument *oneDoc = hit->getSummaryDocument();
        const vector<string> &expectedOneDoc = expectedSummaryDocs[i];
        if (oneDoc->GetFieldCount() != expectedOneDoc.size()) {
            return false;
        }
        for (uint32_t j = 0; j < oneDoc->GetFieldCount(); j++) {
            const ConstString *actualStr = oneDoc->GetFieldValue(j);
            const string &expectedStr = expectedOneDoc[j];
            std::string actualString = std::string(actualStr->data(), actualStr->size());
            if (expectedStr != actualString) {
                return false;
            }
        }
    }
    return true;
}

END_HA3_NAMESPACE(summary);
