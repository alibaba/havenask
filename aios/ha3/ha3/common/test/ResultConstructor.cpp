#include <ha3/common/test/ResultConstructor.h>
#include <autil/StringUtil.h>
#include <autil/MultiValueCreator.h>
#include <autil/StringTokenizer.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/DistinctInfo.h>
#include <ha3/common/PhaseOneSearchInfo.h>
#include <autil/MultiValueCreator.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, ResultConstructor);

ResultConstructor::ResultConstructor() {
    _sortRefCount = 0;
}

ResultConstructor::~ResultConstructor() {
}

void ResultConstructor::fillHit(Hits *hits, hashid_t pid,
                                uint32_t summaryCount, string summaryNames,
                                string value)
{
    vector<string> nameVect;
    HitSummarySchema *hitSummarySchema = new HitSummarySchema;
    StringTokenizer summaryNamesToken(summaryNames, ",",
            StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = summaryNamesToken.begin();
         it != summaryNamesToken.end(); ++it)
    {
        hitSummarySchema->declareSummaryField(*it);
    }
    hits->addHitSummarySchema(hitSummarySchema);

    StringTokenizer hitsTokens(value, "#",
                               StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = hitsTokens.begin();
         it != hitsTokens.end(); ++it)
    {
        StringTokenizer hitTokens(*it, ",",
                StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(hitTokens.getNumTokens() > (size_t)2);
        string clusterName = hitTokens[0];
        docid_t docid;
        StringUtil::strToInt32(hitTokens[1].c_str(), docid);
        HitPtr hitPtr(new Hit(docid));
        hitPtr->setClusterName(clusterName);
        hitPtr->setHashId(pid);
        SummaryHit *summaryHit = new SummaryHit(hitSummarySchema, NULL);
        for (uint32_t i = 2; i < hitTokens.getNumTokens(); ++i) {
            summaryHit->setFieldValue(i - 2, hitTokens[i].data(), hitTokens[i].size());
        }
        hitPtr->setSummaryHit(summaryHit);
        hitPtr->setIp(1234);
        hits->addHit(hitPtr);
    }
}

void ResultConstructor::fillPhaseOneSearchInfo(ResultPtr resultPtr,
        const string &infoStr)
{
    vector<int64_t> valueVec;
    StringUtil::fromString(infoStr, valueVec, ",");
    assert(valueVec.size() == 10);

    PhaseOneSearchInfo *searchInfo = new PhaseOneSearchInfo(
            (uint32_t)valueVec[0], (uint32_t)valueVec[1],
            (uint32_t)valueVec[2], (uint32_t)valueVec[3],
            (uint32_t)valueVec[4], valueVec[5], valueVec[6],
            valueVec[7], valueVec[8], (uint32_t)valueVec[9]);
    resultPtr->setPhaseOneSearchInfo(searchInfo);
}

void ResultConstructor::fillPhaseTwoSearchInfo(ResultPtr resultPtr,
        const string &infoStr)
{
    vector<int64_t> valueVec;
    StringUtil::fromString(infoStr, valueVec, ",");
    assert(valueVec.size() == 1);
    PhaseTwoSearchInfo *searchInfo = new PhaseTwoSearchInfo;
    searchInfo->summaryLatency = valueVec[0];
    resultPtr->setPhaseTwoSearchInfo(searchInfo);
}

void ResultConstructor::declareMatchDocsVariable(
        common::Ha3MatchDocAllocator *matchDocAllocator,
        uint8_t phaseOneInfoFlag,
        bool declareVector)
{
    matchDocAllocator->initPhaseOneInfoReferences(phaseOneInfoFlag);
    matchDocAllocator->declare<hashid_t>(HASH_ID_REF, SL_QRS);
    matchDocAllocator->declare<clusterid_t>(CLUSTER_ID_REF, SL_QRS);
    // declare attribute
    matchdoc::ValueType valueType;
    matchdoc::Reference<MultiInt32> *ref0 = matchDocAllocator->declare<MultiInt32>(
            "ref0", SL_ATTRIBUTE);
    valueType = ref0->getValueType();        
    valueType._ha3ReservedFlag = 1;
    ref0->setValueType(valueType);
    
    matchdoc::Reference<int64_t> *ref1 = matchDocAllocator->declare<int64_t>(
            "ref1", SL_ATTRIBUTE);
    valueType = ref1->getValueType();        
    valueType._ha3ReservedFlag = 1;
    ref1->setValueType(valueType);
    
    // declare user data
    matchDocAllocator->declare<double>(
            PROVIDER_VAR_NAME_PREFIX + "ref2", SL_VARIABLE);
    matchDocAllocator->declare<int32_t>(
            PROVIDER_VAR_NAME_PREFIX + "ref3", SL_VARIABLE);
    matchDocAllocator->declare<std::string>(
            PROVIDER_VAR_NAME_PREFIX + "ref4", SL_VARIABLE);

    matchDocAllocator->declare<MultiChar>("ref5", SL_QRS);

    // declare sort field
    matchDocAllocator->declare<double>(
            "SORT_FIELD_PREFIX_" + StringUtil::toString(_sortRefCount++)
            , SL_QRS);
    matchDocAllocator->declare<int32_t>(
            "SORT_FIELD_PREFIX_" + StringUtil::toString(_sortRefCount++)
            , SL_QRS);
    // declare multi value field
    matchDocAllocator->declare<MultiDouble>(
            PROVIDER_VAR_NAME_PREFIX + "ref8", SL_VARIABLE);
    matchDocAllocator->declare<MultiString>(
            PROVIDER_VAR_NAME_PREFIX + "ref9", SL_VARIABLE);

    // declare vector if needed
    if (declareVector) {
        typedef vector<int8_t> VI8;
        matchDocAllocator->declare<VI8>(
                PROVIDER_VAR_NAME_PREFIX + "ref11", SL_VARIABLE);

        typedef vector<double> VD;
        matchDocAllocator->declare<VD>(
                PROVIDER_VAR_NAME_PREFIX + "ref12", SL_VARIABLE);
    }

    //declare rank trace
    matchDocAllocator->declare<Tracer>(RANK_TRACER_NAME, SL_QRS);
}

void ResultConstructor::assignMatchDocsVariable(
        common::Ha3MatchDocAllocator *matchDocAllocator,
        MatchDocs *matchDocs, bool declareVector,
        autil::mem_pool::Pool *pool)
{
    vector<matchdoc::MatchDoc> &matchDocVec = matchDocs->getMatchDocsVect();
    matchdoc::MatchDoc matchDoc1 = matchDocVec[0];
    matchdoc::MatchDoc matchDoc2 = matchDocVec[1];

    auto clusterIdRef = matchDocAllocator->findReference<clusterid_t>(CLUSTER_ID_REF);
    clusterIdRef->set(matchDoc1, 0);
    clusterIdRef->set(matchDoc2, 1);
    //write value into slab
    auto pk64Ref = matchDocs->getPrimaryKey64Ref();
    if (pk64Ref) {
        pk64Ref->set(matchDoc1, 654321);
        pk64Ref->set(matchDoc2, 123456);
    }
    auto pk128Ref = matchDocs->getPrimaryKey128Ref();
    if (pk128Ref) {
        pk128Ref->set(matchDoc1, primarykey_t(654321));
        pk128Ref->set(matchDoc2, primarykey_t(123456));
    }
    static int32_t INT32_DATA[] = {12, 323, 53};
    MultiInt32 multiInt32_1;
    MultiInt32 multiInt32_2;
    multiInt32_1.init(MultiValueCreator::createMultiValueBuffer(INT32_DATA, 3, pool));
    multiInt32_2.init(MultiValueCreator::createMultiValueBuffer(INT32_DATA + 1, 2, pool));
    auto ref0 = matchDocAllocator->findReference<MultiInt32>("ref0");
    assert(ref0);
    ref0->set(matchDoc1, multiInt32_1);
    ref0->set(matchDoc2, multiInt32_2);

    auto ref1 = matchDocAllocator->findReference<int64_t>("ref1");
    assert(ref1);
    ref1->set(matchDoc2, 12);
    ref1->set(matchDoc2, 32);

    auto ref2 = matchDocAllocator->findReference<double>(PROVIDER_VAR_NAME_PREFIX + "ref2");
    assert(ref2);
    double checkDouble = 1.5f;
    ref2->set(matchDoc1, checkDouble);
    ref2->set(matchDoc2, checkDouble);
    auto ref3 = matchDocAllocator->findReference<int32_t>(
            PROVIDER_VAR_NAME_PREFIX + "ref3");
    assert(ref3);
    int32_t checkInt = 777;
    ref3->set(matchDoc1, checkInt);
    ref3->set(matchDoc2, checkInt);
    auto ref4 = matchDocAllocator->findReference<string>(
            PROVIDER_VAR_NAME_PREFIX + "ref4");
    assert(ref4);
    static std::string value = "abc";
    ref4->set(matchDoc2, value);
    ref4->set(matchDoc1, value);

    auto ref5 = matchDocAllocator->findReference<MultiChar>("ref5");
    assert(ref5);
    MultiChar multiChar;
    char tmpBuf[] = "abcd";
    multiChar.init(MultiValueCreator::createMultiValueBuffer(tmpBuf, 4, pool));
    ref5->set(matchDoc2, multiChar);
    ref5->set(matchDoc1, multiChar);

    auto resultRefVec = matchDocAllocator->getAllNeedSerializeReferences(SL_NONE + 1);
    for (auto ref : resultRefVec) {
        std::cout << "name: " << ref->getName() << std::endl;
    }
    auto ref7 = matchDocAllocator->findReference<double>(
            "SORT_FIELD_PREFIX_" + StringUtil::toString(_sortRefCount - 2));
    assert(ref7);
    ref7->set(matchDoc1, 11.1);
    ref7->set(matchDoc2, 99.9);
    auto ref8 = matchDocAllocator->findReference<int32_t>(
            "SORT_FIELD_PREFIX_" + StringUtil::toString(_sortRefCount - 1));
    assert(ref8);
    ref8->set(matchDoc1, 233);
    ref8->set(matchDoc2, 345);

    auto ref9 = matchDocAllocator->findReference<MultiDouble>(
            PROVIDER_VAR_NAME_PREFIX + "ref8");
    assert(ref9);
    static double DOUBLE_DATA[] = {1.1, 2.2, 3.4};
    MultiDouble multiDouble_1;
    multiDouble_1.init(MultiValueCreator::createMultiValueBuffer(DOUBLE_DATA, 3, pool));
    MultiDouble multiDouble_2;
    multiDouble_2.init(MultiValueCreator::createMultiValueBuffer(DOUBLE_DATA, 2, pool));
    ref9->set(matchDoc1, multiDouble_1);
    ref9->set(matchDoc2, multiDouble_2);

    auto ref10 = matchDocAllocator->findReference<MultiString>(
            PROVIDER_VAR_NAME_PREFIX + "ref9");
    assert(ref10);

    vector<string> stringVec;
    stringVec.push_back("abc");
    stringVec.push_back("123456");

    MultiString multiString;
    multiString.init(MultiValueCreator::createMultiStringBuffer(stringVec, pool));
    ref10->set(matchDoc1, multiString);
    ref10->set(matchDoc2, multiString);

    // for vector
    if (declareVector) {
        typedef vector<int8_t> VI;
        typedef vector<double> VD;

        auto ref11 = matchDocAllocator->findReference<VI>(PROVIDER_VAR_NAME_PREFIX + "ref11");
        assert(ref11);

        auto ref12 = matchDocAllocator->findReference<VD>(PROVIDER_VAR_NAME_PREFIX + "ref12");
        assert(ref12);

        VI vi1, vi2;
        VD vd1, vd2;
        vi1.push_back(1);
        vi1.push_back(2);
        vi2.push_back(3);
        vi2.push_back(4);

        ref11->set(matchDoc1, vi1);
        ref11->set(matchDoc2, vi2);

        vd1.push_back(1.1);
        vd2.push_back(2.2);

        ref12->set(matchDoc1, vd1);
        ref12->set(matchDoc2, vd2);
    }
    auto tracerRef = matchDocAllocator->findReference<Tracer>(RANK_TRACER_NAME);
    string tracerStr("test tracer");
    auto tracer1 = tracerRef->getPointer(matchDoc1);
    auto tracer2 = tracerRef->getPointer(matchDoc2);
    assert(tracer1);
    assert(tracer2);
    tracer1->trace(tracerStr);
    tracer2->trace(tracerStr);
}

void ResultConstructor::prepareMatchDocs(
        MatchDocs *matchDocs,
        autil::mem_pool::Pool* pool,
        uint8_t phaseOneInfoFlag,
        bool declareVector)
{
    matchDocs->setTotalMatchDocs(10);
    matchDocs->setActualMatchDocs(9);

    auto matchDocAllocator = new common::Ha3MatchDocAllocator(pool);
    //construct 'VariableSlabSerializer' for serialization
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

    declareMatchDocsVariable(matchDocAllocator, phaseOneInfoFlag,
                             declareVector);
    //allocate slab
    matchdoc::MatchDoc matchDoc1 = matchDocAllocator->allocate(100);
    matchdoc::MatchDoc matchDoc2 = matchDocAllocator->allocate(101);
    matchDocs->addMatchDoc(matchDoc1);
    matchDocs->addMatchDoc(matchDoc2);
    matchDocs->setGlobalIdInfo(1, versionid_t(2), 3, 4, phaseOneInfoFlag);

    auto hashidRef = matchDocAllocator->findReference<hashid_t>(HASH_ID_REF);
    hashidRef->set(matchDoc2, 2);

    vector<matchdoc::MatchDoc> matchDocVec;
    matchDocVec.push_back(matchDoc1);
    matchDocVec.push_back(matchDoc2);
    assignMatchDocsVariable(matchDocAllocator, matchDocs, declareVector, pool);
}

// use int32_t type for sort reference
void ResultConstructor::fillMatchDocs(
        MatchDocs *matchDocs, uint32_t sortReferCount,
        uint32_t distReferCount, uint32_t totalMatchDocs,
        uint32_t actualMatchDocs, autil::mem_pool::Pool* pool,
        const string &value, bool sortFlag, const string &primaryKeyValues,
        FullIndexVersion fullIndexVersion, bool isDistinctFiltered,
        uint32_t extraDistReferCount, uint32_t uniqKeyReferCount,
        uint32_t personalReferCount)
{
    _sortRefCount = 0;
    assert(distReferCount < 2);
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(pool);
    common::Ha3MatchDocAllocatorPtr allocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(allocatorPtr);

    //set MatchDocs data member
    matchDocs->setTotalMatchDocs(totalMatchDocs);
    matchDocs->setActualMatchDocs(actualMatchDocs);
    //declare sort reference
    std::vector<matchdoc::ReferenceBase *> sortReferVec;
    std::vector<matchdoc::ReferenceBase *> distinctReferVec;
    matchdoc::ReferenceBase* distinctFilterRefer = NULL;
    auto hashidRef = matchDocAllocator->declare<hashid_t>(HASH_ID_REF,
            common::HA3_GLOBAL_INFO_GROUP, SL_QRS);
    matchDocAllocator->declare<clusterid_t>(CLUSTER_ID_REF,
            CLUSTER_ID_REF_GROUP);
    for (uint32_t i = 0; i < sortReferCount; ++i) {
        assert(matchDocAllocator);
        matchdoc::Reference<int32_t>* refer =
            matchDocAllocator->declare<int32_t>(
                    "SORT_FIELD_PREFIX_" + StringUtil::toString(_sortRefCount++),
                    SL_QRS);
        sortReferVec.push_back(refer);
    }

    //declare distinct reference
    for (uint32_t i = 0; i < distReferCount; ++i) {
        auto refer = matchDocAllocator->declare<int64_t>(
                string("distinct") + StringUtil::toString(i), SL_QRS);
        distinctReferVec.push_back(refer);

        //declare distinctInfo
        matchDocAllocator->declare<DistinctInfo>(DISTINCT_INFO_REF);
    }

    if (isDistinctFiltered) {
        auto refer = matchDocAllocator->declare<bool>(
                string("(distinct0=666)"), SL_QRS);
        distinctFilterRefer = refer;
    }

    vector<matchdoc::Reference<int32_t>*> extraDistKeyRefVect;
    for (uint32_t i = 0; i < extraDistReferCount; i++) {
        stringstream ss;
        ss << "extra_dist_key" << i;
        auto refer = matchDocAllocator->declare<int32_t>(ss.str(), SL_QRS);
        extraDistKeyRefVect.push_back(refer);
    }

    matchdoc::Reference<int32_t>* uniqKeyRef = NULL;
    if (uniqKeyReferCount > 0) {
        uniqKeyRef = matchDocAllocator->declare<int32_t>("extra_dist_uniqkey",
                SL_QRS);
    }

    vector<matchdoc::Reference<score_t>*> personalRefVect;
    for (uint32_t i = 0; i < personalReferCount; i++) {
        stringstream ss;
        ss << PROVIDER_VAR_NAME_PREFIX + "personal_score_" << i;
        matchdoc::Reference<score_t>* refer =
            matchDocAllocator->declare<score_t>(ss.str(), SL_QRS);
        personalRefVect.push_back(refer);
    }

    matchdoc::Reference<uint64_t> * pk64Ref = NULL;

    matchdoc::Reference<FullIndexVersion> * fullIndexVersionRef =
        matchDocAllocator->declare<FullIndexVersion>(FULLVERSION_REF, SL_QRS);
    vector<uint64_t> pkVec;
    if (!primaryKeyValues.empty()) {
        pk64Ref = matchDocAllocator->declare<uint64_t>(PRIMARYKEY_REF, SL_QRS);
        StringUtil::fromString(primaryKeyValues, pkVec, ",");
    }
    vector<uint64_t>::const_iterator pkIt = pkVec.begin();

    //tokenizer and fill matchDocs data
    StringTokenizer matchdocsTokens(value, "#",
                                    StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = matchdocsTokens.begin();
         it != matchdocsTokens.end(); ++it)
    {
        StringTokenizer referTokens(*it, ",",
                StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        size_t referCount = 1 + sortReferCount + distReferCount +
                            extraDistReferCount + uniqKeyReferCount + personalReferCount;
        if (isDistinctFiltered) {
            referCount++;
        }
        assert(referCount == referTokens.getNumTokens() - 1);

        //write docid
        const string &strDocid = referTokens[0];
        docid_t docid;
        StringUtil::strToInt32(strDocid.c_str(), docid);
        matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(docid);

        //write hashid
        const string &strPid = referTokens[1];
        int32_t hashid;
        StringUtil::strToInt32(strPid.c_str(), hashid);
        hashidRef->set(matchDoc, hashid);
        //write full index version
        fullIndexVersionRef->set(matchDoc, fullIndexVersion);

        //write primaryKey
        if (pkIt != pkVec.end() ) {
            pk64Ref->set(matchDoc, *pkIt++);
        }

        uint32_t i = 2;
        //write sort value
        for (std::vector<matchdoc::ReferenceBase *>::iterator it = sortReferVec.begin();
             it != sortReferVec.end(); ++it)
        {
            int32_t value;
            StringUtil::strToInt32(referTokens[i].c_str(), value);//sort
            const matchdoc::Reference<int32_t>* refer
                = dynamic_cast<const matchdoc::Reference<int32_t>*>((*it));
            refer->set(matchDoc, value);
            i++;
        }
        HA3_LOG(TRACE3, "after fill sort");
        HA3_LOG(TRACE3, "fill dist");
        //write distinct value
        if (distReferCount) {
            int64_t value;
            StringUtil::strToInt64(referTokens[i].c_str(), value);//distinct
            const matchdoc::Reference<int64_t>* refer
                = dynamic_cast<const matchdoc::Reference<int64_t>*>(distinctReferVec[0]);
            refer->set(matchDoc, value);
            i++;
        }

        if (isDistinctFiltered) {
            int8_t value = StringUtil::fromString<int8_t>(referTokens[i].c_str());//distinct filter
            const matchdoc::Reference<bool>* refer
                = dynamic_cast<const matchdoc::Reference<bool>*>(distinctFilterRefer);
            refer->set(matchDoc, value);
            i++;
        }

        for (auto it = extraDistKeyRefVect.begin(); it != extraDistKeyRefVect.end(); it++) {
            int32_t value = StringUtil::fromString<int32_t>(referTokens[i].c_str());
            (*it)->set(matchDoc, value);
            i++;
        }

        if (uniqKeyRef) {
            int32_t value = StringUtil::fromString<int32_t>(referTokens[i].c_str());
            uniqKeyRef->set(matchDoc, value);
            i++;
        }

        for (auto it = personalRefVect.begin(); it != personalRefVect.end(); it++) {
            score_t value = StringUtil::fromString<score_t>(
                    referTokens[i].c_str());
            (*it)->set(matchDoc, value);
            i++;
        }
        matchDocs->addMatchDoc(matchDoc);
    }
}

// use float type for sort reference
void ResultConstructor::fillMatchDocs2(MatchDocs *matchDocs, uint32_t sortReferCount,
                                       uint32_t distReferCount, uint32_t totalMatchDocs,
                                       autil::mem_pool::Pool* pool,
                                       const string &value)
{
    assert(distReferCount < 2);
    _sortRefCount = 0;
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(pool);
    common::Ha3MatchDocAllocatorPtr allocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(allocatorPtr);

    //set MatchDocs data member
    matchDocs->setTotalMatchDocs(totalMatchDocs);

    auto hashidRef = matchDocAllocator->declare<hashid_t>(HASH_ID_REF,
            common::HA3_GLOBAL_INFO_GROUP, SL_QRS);
    matchDocAllocator->declare<clusterid_t>(CLUSTER_ID_REF,
            CLUSTER_ID_REF_GROUP);
    //declare sort reference
    std::vector<matchdoc::ReferenceBase *> sortReferVec;
    for (uint32_t i = 0; i < sortReferCount; ++i) {
        assert(matchDocAllocator);
        matchdoc::Reference<float>* refer =
            matchDocAllocator->declare<float>(
                    "SORT_FIELD_PREFIX_" + StringUtil::toString(_sortRefCount++)
                    , SL_QRS);
        sortReferVec.push_back(refer);
    }

    //declare distinct reference
    std::vector<matchdoc::ReferenceBase *> distReferVec;
    for (uint32_t i = 0; i < distReferCount; ++i) {
        matchdoc::Reference<int64_t>* refer =
            matchDocAllocator->declare<int64_t>(string("distinct") +
                    StringUtil::toString(i), SL_QRS);
        distReferVec.push_back(refer);
    }

    //tokenizer and fill matchDocs data
    StringTokenizer matchdocsTokens(value, "#",
                                    StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = matchdocsTokens.begin();
         it != matchdocsTokens.end(); ++it)
    {
        StringTokenizer referTokens(*it, ",",
                StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert((size_t)(1 + sortReferCount + distReferCount) == referTokens.getNumTokens() - 1) ;

        //write docid
        const string &strDocid = referTokens[0];
        docid_t docid;
        StringUtil::strToInt32(strDocid.c_str(), docid);
        matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(docid);

        //write hashid
        const string &strPid = referTokens[1];
        int32_t hashid;
        StringUtil::strToInt32(strPid.c_str(), hashid);
        hashidRef->set(matchDoc, hashid);

        uint32_t i = 1;
        //write sort value
        int32_t j = 0;
        for (; i < sortReferCount + 1; ++i, ++j) {
            float value;
            StringUtil::strToFloat(referTokens[i+1].c_str(), value);//sort
            const matchdoc::Reference<float>* refer
                = dynamic_cast<const matchdoc::Reference<float>*>(sortReferVec[j]);
            refer->set(matchDoc, value);
        }
        HA3_LOG(TRACE3, "after fill sort");
        HA3_LOG(TRACE3, "fill dist");
        //write distinct value
        if (distReferCount) {
            int64_t value;
            StringUtil::strToInt64(referTokens[i].c_str(), value);//distinct
            const matchdoc::Reference<int64_t>* refer
                = dynamic_cast<const matchdoc::Reference<int64_t>*>(distReferVec[0]);
            refer->set(matchDoc, value);
        }

        matchDocs->addMatchDoc(matchDoc);
    }
}

void ResultConstructor::fillAggregateResult(ResultPtr resultPtr,
        const string &funNames,
        const string &slabValue,
        autil::mem_pool::Pool *pool,
        const string &groupExprStr,
        const string &funParameters)
{
    AggregateResultPtr aggResultPtr(new AggregateResult(groupExprStr));
    matchdoc::MatchDocAllocatorPtr allocatorPtr(new matchdoc::MatchDocAllocator(pool));
    aggResultPtr->setMatchDocAllocator(allocatorPtr);

    //decalre all the aggregate function's reference
    StringTokenizer funNamesTokenizer(funNames, ",",
            StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    int i = 0;
    auto groupKeyRef = allocatorPtr->declare<string>(
            GROUP_KEY_REF, SL_CACHE);
    for (StringTokenizer::Iterator it = funNamesTokenizer.begin();
         it != funNamesTokenizer.end(); ++it, ++i)
    {
        aggResultPtr->addAggFunName(*it);
        allocatorPtr->declare<int64_t>(*it + StringUtil::toString(i), SL_CACHE);
    }

    if (funParameters != "") {
        StringTokenizer funParametersTokenizer(funParameters, ",", StringTokenizer::TOKEN_TRIM);
        for (StringTokenizer::Iterator it = funParametersTokenizer.begin();
             it != funParametersTokenizer.end(); ++it)
        {
            aggResultPtr->addAggFunParameter(*it);
        }
    }

    //fill the value into VariableSlab
    StringTokenizer slabValueTokenizer(slabValue, "#",
            StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = slabValueTokenizer.begin();
         it != slabValueTokenizer.end(); ++it, ++i)
    {
        StringTokenizer slabValueTokens(*it, ",",
                StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(funNamesTokenizer.getNumTokens() + 1 == slabValueTokens.getNumTokens());
        matchdoc::MatchDoc slab = allocatorPtr->allocate();
        auto referenceVec = allocatorPtr->getAllNeedSerializeReferences(
                SL_NONE + 1);
        int32_t value;
        int32_t j = 0;
        for (auto referIt = referenceVec.begin() + 1;
             referIt != referenceVec.end(); ++referIt, ++j) {
            StringUtil::strToInt32(slabValueTokens[j+1].c_str(), value);
            auto refer = dynamic_cast<const matchdoc::Reference<int64_t>*>(*referIt);
            refer->set(slab, (int64_t)value);
        }
        groupKeyRef->set(slab, slabValueTokens[0]);
        aggResultPtr->addAggFunResults(slab);
    }
    aggResultPtr->constructGroupValueMap();
    resultPtr->addAggregateResult(aggResultPtr);
}

void ResultConstructor::getSortReferences(matchdoc::MatchDocAllocator *allocator,
        matchdoc::ReferenceVector &refVec)
{
    for (uint32_t i = 0; i < _sortRefCount; i++) {
        string refName = "SORT_FIELD_PREFIX_" + StringUtil::toString(i);
        matchdoc::ReferenceBase *ref =
            allocator->findReferenceWithoutType(refName);
        refVec.push_back(ref);
    }
}

END_HA3_NAMESPACE(common);
