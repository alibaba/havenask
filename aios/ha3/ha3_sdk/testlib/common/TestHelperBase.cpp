#include <autil/StringUtil.h>
#include <ha3_sdk/testlib/common/TestHelperBase.h>
#include <ha3/rank/TermMatchData.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <ha3_sdk/testlib/index/IndexPartitionReaderWrapperCreator.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, TestHelperBase);

TestHelperBase::TestHelperBase(const std::string &testPath)
    : _testPath(testPath)
{
    _pool = new Pool;
    _attrExprCreator = NULL;
    _mdAllocatorPtr.reset(new Ha3MatchDocAllocator(_pool));
    _dataProvider = POOL_NEW_CLASS(_pool, DataProvider);
    _matchDataRef = NULL;
    _simpleMatchDataRef = NULL;
    _requestTracer = NULL;
}

TestHelperBase::~TestHelperBase() {
    POOL_DELETE_CLASS(_attrExprCreator);
    POOL_DELETE_CLASS(_dataProvider);
    deallocateMatchDocs(_matchDocs);
    _mdAllocatorPtr.reset();
    DELETE_AND_SET_NULL(_pool);
    DELETE_AND_SET_NULL(_requestTracer);
}

bool TestHelperBase::init(TestParamBase *param) {
    if (!param->requestStr.empty()) {
        _requestPtr = RequestCreator::prepareRequest(param->requestStr);
        if (_requestPtr) {
            ConfigClause *config = _requestPtr->getConfigClause();
            _requestTracer = config->createRequestTracer("0", "127.0.0.1");
        }
    }
    if (param->numTerms > 0) {
        requireMatchData(param);
    }

    // build index
    _indexWrap.reset(new IndexWrap(_testPath + "_" + autil::StringUtil::toString(autil::TimeUtility::currentTime())));
    if (!_invertParam.addFakeIndexData(param->fakeIndex.attributes)) {
        return false;
    }
    _indexWrap->addInvertedTable(&_invertParam);
    _indexWrap->buildIndex();

    _partitionReaderSnapshot = _indexWrap->CreateSnapshot();
    const KeyValueMap *kvPair = NULL;
    if (_requestPtr && _requestPtr->getConfigClause()) {
        kvPair = &(_requestPtr->getConfigClause()->getKVPairs());
    }
    _functionProvider.reset(new suez::turing::FunctionProvider(_mdAllocatorPtr.get(),
                    _pool, nullptr, nullptr, _partitionReaderSnapshot.get(), kvPair));

    _attrExprCreator = POOL_NEW_CLASS(_pool, suez::turing::AttributeExpressionCreator,
            _pool, _mdAllocatorPtr.get(), _invertParam.tableName,
            _partitionReaderSnapshot.get(), _indexWrap->createTableInfo(_invertParam.tableName),
            _virtualAttributes, NULL, CavaPluginManagerPtr(), _functionProvider.get());
    _indexPartReader = _partitionReaderSnapshot->GetIndexPartitionReader(_invertParam.tableName);

    // TODO
    // _auxTableReaderCreator.reset(new HA3_NS(search)::AuxTableReaderCreator());
    return true;
}

void TestHelperBase::fillSearchPluginResource(SearchPluginResource *resource)
{
    resource->pool = _pool;
    resource->request = _requestPtr.get();
    if (_requestPtr && _requestPtr->getConfigClause()) {
        resource->kvpairs = _requestPtr->getConfigClause()->getKeyValueMapPtr().get();
    }
    resource->globalMatchData.setDocCount(_indexPartReader->GetPartitionInfo()->GetTotalDocCount());
    resource->attrExprCreator = _attrExprCreator;
    resource->dataProvider = _dataProvider;
    resource->matchDocAllocator = _mdAllocatorPtr;
    resource->requestTracer = _requestTracer;
    resource->partitionReaderSnapshot = _partitionReaderSnapshot.get();
}

void TestHelperBase::requireMatchData(TestParamBase *param) {
    uint32_t numTerms = param->numTerms;
    if (!param->matchDataStr.empty() && !param->simpleMatchDataStr.empty()) {
        HA3_LOG(ERROR, "can't require MatchData and SimpleMatchData at the same time");
        assert(false);
    }
    if (!param->matchDataStr.empty()) {
        uint32_t matchDataSize = HA3_NS(rank)::MatchData::sizeOf(numTerms);

        _matchDataRef = _mdAllocatorPtr->declare<MatchData>(MATCH_DATA_REF, SL_NONE, matchDataSize);
    }
    if (!param->simpleMatchDataStr.empty()) {
        uint32_t simpleMatchDataSize = HA3_NS(rank)::SimpleMatchData::sizeOf(numTerms);

        _simpleMatchDataRef = _mdAllocatorPtr->declare<SimpleMatchData>(
                SIMPLE_MATCH_DATA_REF, SL_NONE, simpleMatchDataSize);
    }
}

bool TestHelperBase::fillMatchDatas(const string &inputStr,
                                      const vector<matchdoc::MatchDoc> &matchDocs)
{
    vector<string> docMatchDatas;
    StringUtil::fromString(inputStr, docMatchDatas, ";");
    if (docMatchDatas.size() != matchDocs.size()) {
        return false;
    }
    for (size_t i = 0; i < docMatchDatas.size(); i++) {
        const string &termMatchDataStr = docMatchDatas[i];
        vector<vector<string> > strVecVec;
        StringUtil::fromString(termMatchDataStr, strVecVec, ":", ",");
        MatchData &matchData = _matchDataRef->getReference(matchDocs[i]);
        matchData.setMaxNumTerms(10);
        for (size_t j = 0; j < strVecVec.size(); j++) {
            const vector<string> &termInfoVec = strVecVec[j];
            assert(termInfoVec.size() == 4);
            HA3_NS(rank)::TermMatchData &tmd = matchData.nextFreeMatchData();
            tmd.setFirstOcc(StringUtil::fromString<pos_t>(termInfoVec[0]));
            tmd.setDocPayload(StringUtil::fromString<docpayload_t>(termInfoVec[1]));
            tmd.setFieldBitmap(StringUtil::fromString<fieldbitmap_t>(termInfoVec[2]));
            tmd.setMatched(StringUtil::fromString<int8_t>(termInfoVec[3]));
        }
    }
    return true;
}

bool TestHelperBase::fillSimpleMatchDatas(const string &inputStr,
        const vector<matchdoc::MatchDoc> &matchDocs)
{
    vector<vector<bool> > docSimpleMatchDatas;
    StringUtil::fromString(inputStr, docSimpleMatchDatas, ",", ";");
    if (docSimpleMatchDatas.size() != matchDocs.size()) {
        return false;
    }
    for (size_t i = 0; i < docSimpleMatchDatas.size(); i++) {
        SimpleMatchData &simpleMatchData =
            _simpleMatchDataRef->getReference(matchDocs[i]);
        const vector<bool> &oneDocMatchInfos = docSimpleMatchDatas[i];
        for (size_t j = 0; j < oneDocMatchInfos.size(); j++) {
            simpleMatchData.setMatch(j, oneDocMatchInfos[j]);
        }
    }
    return true;
}

void TestHelperBase::allocateMatchDocs(const string &docIdStr)
{
    MatchDocCreator::allocateMatchDocs(docIdStr, _matchDocs, _mdAllocatorPtr.get());
}

void TestHelperBase::allocateMatchDocs(const string &docIdStr, vector<matchdoc::MatchDoc> &matchDocVec)
{
    MatchDocCreator::allocateMatchDocs(docIdStr, matchDocVec, _mdAllocatorPtr.get());
}

void TestHelperBase::deallocateMatchDocs(const vector<matchdoc::MatchDoc> &matchDocs) {
    MatchDocCreator::deallocateMatchDocs(matchDocs, _mdAllocatorPtr.get());
}


END_HA3_NAMESPACE(common);
