#include <suez/turing/expression/util/VirtualAttrConvertor.h>
#include <ha3/common/PBResultFormatter.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <ha3/common/Tracer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, PBResultFormatter);

#define FILL_VALUE(type_name, value_type)                               \
    if (!attrItem->isMultiValue()) {                                    \
        const AttributeItemTyped<type_name> *attrItemTyped = static_cast<const AttributeItemTyped<type_name> *>(attrItem); \
        assert(attrItemTyped);                                          \
        const type_name &v = attrItemTyped->getItem();                  \
        value->add_##value_type(v);                                     \
    } else {                                                            \
        const AttributeItemTyped<vector<type_name> > *attrItemTyped = static_cast<const AttributeItemTyped<vector<type_name> > *>(attrItem); \
        assert(attrItemTyped);                                          \
        const vector<type_name> &multiValue = attrItemTyped->getItem(); \
        value->mutable_##value_type()->Reserve(multiValue.size());      \
        for (size_t i = 0; i < multiValue.size(); ++i) {                \
            value->add_##value_type(multiValue[i]);                     \
        }                                                               \
    }

#define FILL_INT_ATTRIBUTE(vt)                                          \
    case vt:                                                            \
    {                                                                   \
        typedef VariableTypeTraits<vt, false>::AttrItemType T;          \
        FILL_VALUE(T, int64value);                                      \
        break;                                                          \
    }

#define FILL_DOUBLE_ATTRIBUTE(vt)                                       \
    case vt:                                                            \
    {                                                                   \
        typedef VariableTypeTraits<vt, false>::AttrItemType T;          \
        FILL_VALUE(T, doublevalue);                                     \
        break;                                                          \
    }

#define FILL_ATTRIBUTE(vt, Type, typeattributes, typevalue)             \
    case vt:                                                            \
    {                                                                   \
        typedef VariableTypeTraits<vt, false>::AttrItemType SingleType; \
        typedef vector<SingleType> MultiType;                           \
        typedef MultiValueType<SingleType> MultiExpressType;            \
        Type *attributeValue = pbMatchDocs->add_##typeattributes();     \
        attributeValue->set_key(attributeName);                         \
        attributeValue->set_type(valueType);                            \
        auto isMulti = ref->getValueType().isMultiValue();              \
        if (!isMulti) {                                                 \
            attributeValue->mutable_##typevalue()->Reserve(matchDocs.size()); \
        }                                                               \
        uint32_t offset = 0;                                            \
        if (!isMulti) {                                                 \
            for (uint32_t j = 0; j < matchDocs.size(); j++) {           \
                auto singleRef = static_cast<matchdoc::Reference<SingleType> *>(ref); \
                SingleType value = singleRef->getReference(matchDocs[j]); \
                attributeValue->add_##typevalue(value);                 \
            }                                                           \
        } else {                                                        \
            for (uint32_t j = 0; j < matchDocs.size(); j++) {           \
                MultiType value;                                        \
                if (!ref->getValueType().isStdType()) {                 \
                    auto multiRef = static_cast<matchdoc::Reference<MultiExpressType> *>(ref); \
                    value = Type2AttrItemType<MultiExpressType>::convert(multiRef->getReference(matchDocs[j])); \
                } else {                                                \
                    auto multiRef = static_cast<matchdoc::Reference<MultiType> *>(ref); \
                    value = multiRef->getReference(matchDocs[j]);       \
                }                                                       \
                attributeValue->add_offset(offset);                     \
                for (uint32_t k = 0; k < value.size(); k++) {           \
                    attributeValue->add_##typevalue(value[k]);          \
                }                                                       \
                offset = attributeValue->typevalue##_size();            \
            }                                                           \
        }                                                               \
        break;                                                          \
    }

#define READ_AND_TO_DOUBLE(vt_type)                                     \
    case vt_type:                                                       \
    {                                                                   \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;     \
        sortValue = readNumericData2Double<T>(ref, matchDoc);           \
        break;                                                          \
    }

PBResultFormatter::PBResultFormatter()
    : _option(0)
{
}

PBResultFormatter::~PBResultFormatter() {
}

void PBResultFormatter::format(const ResultPtr &resultPtr, stringstream &ss)
{
    google::protobuf::Arena arena;
    PBResult *pbResult = google::protobuf::Arena::CreateMessage<PBResult>(&arena);
    format(resultPtr, *pbResult);
    pbResult->SerializeToOstream(&ss);
    HA3_LOG(DEBUG, "pb_format:%s",pbResult->DebugString().c_str());
}

void PBResultFormatter::format(const ResultPtr &resultPtr, PBResult &pbResult)
{
    formatTotalTime(resultPtr, pbResult);
    formatMatchDocs(resultPtr, pbResult);
    formatHits(resultPtr, pbResult);
    formatAggregateResults(resultPtr, pbResult);
    formatErrorResult(resultPtr, pbResult);
    formatRequestTracer(resultPtr, pbResult);
    formatMeta(resultPtr, pbResult);
}


void PBResultFormatter::formatHits(const ResultPtr &resultPtr,
                                   PBResult &pbResult)
{
    Hits *hits = resultPtr->getHits();
    if (hits == NULL) {
        HA3_LOG(DEBUG, "There are no hits in the result");
        return;
    }

    PBHits *pbHits = pbResult.mutable_hits();
    uint32_t hitSize = hits->size();

    pbHits->set_numhits(hitSize);
    pbHits->set_totalhits(hits->totalHits());
    pbHits->set_coveredpercent(getCoveredPercent(resultPtr->getCoveredRanges()));
    const vector<SortExprMeta> &sortExprMetas = resultPtr->getSortExprMeta();
    for (vector<SortExprMeta>::const_iterator it = sortExprMetas.begin();
         it != sortExprMetas.end(); ++it)
    {
        SortExprssionMeta *sortExprMeta = pbHits->add_sortexprmetas();
        sortExprMeta->set_sortexprname(it->sortExprName);
        sortExprMeta->set_sortflag(it->sortFlag);
    }

    for (uint32_t i = 0; i < hitSize; i++) {
        HitPtr hitPtr = hits->getHit(i);
        PBHit *pbHit = pbHits->add_hit();
        formatHit(hitPtr, pbHit, hits->isIndependentQuery());
    }

    formatMetaHits(hits, pbHits);
}

void PBResultFormatter::formatMatchDocs(const ResultPtr &resultPtr,
                                        PBResult &pbResult)
{
    auto matchDocs = resultPtr->getMatchDocs();
    if (!matchDocs) {
        HA3_LOG(DEBUG, "There are no matchDocs to format.");
        return;
    }
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    if (!allocatorPtr) {
        HA3_LOG(DEBUG, "Allocator in matchDocs is NULL!");
        return;
    }
    auto &formatMatchDocVec = resultPtr->getMatchDocsForFormat();
    PBMatchDocs *pbMatchDocs = pbResult.mutable_matchdocs();
    assert(pbMatchDocs);
    pbMatchDocs->set_nummatchdocs(formatMatchDocVec.size());
    pbMatchDocs->set_totalmatchdocs(matchDocs->totalMatchDocs());

    const auto &clusterNames = resultPtr->getClusterNames();
    pbMatchDocs->mutable_clusternames()->Reserve(clusterNames.size());
    for (size_t i = 0; i < clusterNames.size(); i++) {
        string *clusterName = pbMatchDocs->add_clusternames();
        *clusterName = clusterNames[i];
    }

    uint32_t matchDocSize = formatMatchDocVec.size();
    auto tracerRef = allocatorPtr->findReference<Tracer>(RANK_TRACER_NAME);
    if (tracerRef) {
        pbMatchDocs->mutable_tracers()->Reserve(matchDocSize);
    }
    pbMatchDocs->mutable_hashids()->Reserve(matchDocSize);
    pbMatchDocs->mutable_docids()->Reserve(matchDocSize);

    auto fullVersionRef = matchDocs->getFullIndexVersionRef();
    if (fullVersionRef) {
        pbMatchDocs->mutable_fullindexversions()->Reserve(matchDocSize);
    }
    auto indexVersionRef = matchDocs->getIndexVersionRef();
    if (indexVersionRef) {
        pbMatchDocs->mutable_indexversions()->Reserve(matchDocSize);
    }
    auto primaryKey128Ref = matchDocs->getPrimaryKey128Ref();
    if (primaryKey128Ref) {
        pbMatchDocs->mutable_pkhighers()->Reserve(matchDocSize);
        pbMatchDocs->mutable_pklowers()->Reserve(matchDocSize);
    }
    auto primaryKey64Ref = matchDocs->getPrimaryKey64Ref();
    if (primaryKey64Ref) {
        pbMatchDocs->mutable_pklowers()->Reserve(matchDocSize);
    }
    auto ipRef = matchDocs->getIpRef();
    if (ipRef) {
        pbMatchDocs->mutable_searcherips()->Reserve(matchDocSize);
    }

    auto hashIdRef = matchDocs->getHashIdRef();
    auto clusterIdRef = matchDocs->getClusterIdRef();
    for (uint32_t i = 0; i < matchDocSize; i++) {
        pbMatchDocs->add_clusterids(clusterIdRef->get(formatMatchDocVec[i]));
        pbMatchDocs->add_hashids(hashIdRef->get(formatMatchDocVec[i]));
        pbMatchDocs->add_docids(formatMatchDocVec[i].getDocId());
        auto matchDoc = formatMatchDocVec[i];
        if (tracerRef) {
            string *rankTraceStr = pbMatchDocs->add_tracers();
            Tracer *rankTrace = tracerRef->getPointer(matchDoc);
            if (rankTrace) {
                *rankTraceStr = rankTrace->getTraceInfo();
            }
        }
        if (fullVersionRef) {
            pbMatchDocs->add_fullindexversions(fullVersionRef->getReference(matchDoc));
        }
        if (indexVersionRef) {
            pbMatchDocs->add_indexversions(indexVersionRef->getReference(matchDoc));
        }
        if (primaryKey128Ref) {
            primarykey_t &pk128 = primaryKey128Ref->getReference(matchDoc);
            pbMatchDocs->add_pkhighers(pk128.value[0]);
            pbMatchDocs->add_pklowers(pk128.value[1]);
        }
        if (primaryKey64Ref) {
            pbMatchDocs->add_pklowers(primaryKey64Ref->getReference(matchDoc));
        }
        if (ipRef) {
            pbMatchDocs->add_searcherips(ipRef->getReference(matchDoc));
        }
    }
    auto attrReferVec = allocatorPtr->getAllNeedSerializeReferences(SL_CACHE);

    auto userDataReferVec = allocatorPtr->getRefBySerializeLevel(SL_VARIABLE);
    formatPBMatchDocVariableReferences(attrReferVec,
            formatMatchDocVec, pbMatchDocs, ATTRIBUTE_TYPE);
    formatPBMatchDocVariableReferences(userDataReferVec,
            formatMatchDocVec, pbMatchDocs, VARIABLE_VALUE_TYPE);
    const auto &sortExprMetas = resultPtr->getSortExprMeta();
    formatPBMatchDocSortValues(sortExprMetas, formatMatchDocVec, pbMatchDocs);
}

void PBResultFormatter::formatPBMatchDocVariableReferences(
        const matchdoc::ReferenceVector &refVec,
        const std::vector<matchdoc::MatchDoc> &matchDocs,
        PBMatchDocs *pbMatchDocs,
        ValueType valueType)
{
    string prefixStr;
    if (ATTRIBUTE_TYPE == valueType) {
        prefixStr = "";
    } else {
        prefixStr = PROVIDER_VAR_NAME_PREFIX;
    }

    size_t PREFIX_SIZE = prefixStr.size();
    map<string, string>::iterator iter;
    for(uint32_t i = 0; i < refVec.size(); i++) {
        auto ref = refVec[i];
        if (ATTRIBUTE_TYPE == valueType && ref->getValueType()._ha3ReservedFlag == 0) {
            continue;
        }
        string attributeName = ref->getName();
        assert(attributeName.substr(0, PREFIX_SIZE) == prefixStr);
        attributeName = attributeName.substr(PREFIX_SIZE);
        if (ATTRIBUTE_TYPE == valueType) {
            iter = _attrNameMap.find(attributeName);
            if (iter != _attrNameMap.end()) {
                attributeName = iter->second;
            }
        }
        auto vt = ref->getValueType().getBuiltinType();
        switch (vt) {
            FILL_ATTRIBUTE(vt_bool, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_int8, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_int16, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_int32, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_int64, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_uint8, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_uint16, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_uint32, PBInt64Attribute, int64attrvalues, int64value);
            FILL_ATTRIBUTE(vt_uint64, PBInt64Attribute, int64attrvalues, int64value);

            FILL_ATTRIBUTE(vt_float, PBDoubleAttribute, doubleattrvalues, doublevalue);
            FILL_ATTRIBUTE(vt_double, PBDoubleAttribute, doubleattrvalues, doublevalue);
        case vt_string:
        {
            typedef VariableTypeTraits<vt_string,
                                       false>::AttrItemType T;
            PBBytesAttribute *value = pbMatchDocs->add_bytesattrvalues();
            value->set_key(attributeName);
            value->set_type(valueType);
            uint32_t offset = 0;
            for (uint32_t j = 0; j < matchDocs.size(); j++) {
                const auto &attrItemPtr =
                    Ha3MatchDocAllocator::createAttributeItem(ref, matchDocs[j]);
                AttributeItem *attrItem = attrItemPtr.get();
                value->add_offset(offset);
                FILL_VALUE(T, bytesvalue);
                offset = value->bytesvalue_size();
            }
            break;
        }
        case vt_hash_128:
        {
            PBBytesAttribute *value = pbMatchDocs->add_bytesattrvalues();
            value->set_key(attributeName);
            value->set_type(valueType);
            for (uint32_t j = 0; j < matchDocs.size(); j++) {
                const auto &attrItemPtr =
                    Ha3MatchDocAllocator::createAttributeItem(ref, matchDocs[j]);
                AttributeItem *attrItem = attrItemPtr.get();
                assert(!attrItem->isMultiValue());
                const auto *attrItemTyped = static_cast<
                    const AttributeItemTyped<primarykey_t> *>(attrItem);
                assert(attrItemTyped);
                const primarykey_t &v = attrItemTyped->getItem();
                value->add_bytesvalue(StringUtil::toString(v));
            }
            break;
        }
        default:
            assert(false);
            break;
        }
    }
}

void PBResultFormatter::formatPBMatchDocSortValues(
        const vector<SortExprMeta> &sortExprMetas,
        const std::vector<matchdoc::MatchDoc> &matchDocs,
        PBMatchDocs *pbMatchDocs)
{
    PBSortValues *sortValues = pbMatchDocs->mutable_sortvalues();
    sortValues->set_dimensioncount(sortExprMetas.size());
    for (vector<SortExprMeta>::const_iterator it = sortExprMetas.begin();
         it != sortExprMetas.end(); ++it)
    {
        SortExprssionMeta *sortExprMeta = sortValues->add_sortexprmetas();
        sortExprMeta->set_sortexprname(it->sortExprName);
        sortExprMeta->set_sortflag(it->sortFlag);
    }
    for (uint32_t i = 0; i < matchDocs.size(); i++) {
        for (vector<SortExprMeta>::const_iterator it = sortExprMetas.begin();
             it != sortExprMetas.end(); ++it) {
            auto matchDoc = matchDocs[i];
            const auto *ref = it->sortRef;
            auto vt = ref->getValueType().getBuiltinType();
            double sortValue = 0.0;
            switch(vt) {
                NUMERIC_VARIABLE_TYPE_MACRO_HELPER(READ_AND_TO_DOUBLE);
            default:
                break;
            }
            sortValues->add_sortvalues(sortValue);
        }
    }
}

void PBResultFormatter::formatMetaHits(const Hits *hits, PBHits *pbHits) {
    const MetaHitMap& metaHitMap = hits->getMetaHitMap();
    for (MetaHitMap::const_iterator it = metaHitMap.begin();
         it != metaHitMap.end(); ++it)
    {
        const MetaHit &metaHit = it->second;
        if (metaHit.empty()) {
            continue;
        }
        PBMetaHitMap *pbMetaMap = pbHits->add_metahitmap();
        pbMetaMap->set_metahitkey(it->first);
        for (MetaHit::const_iterator it2 = metaHit.begin();
             it2 != metaHit.end(); ++it2)
        {
            PBKVPair *metaHitVlaue = pbMetaMap->add_metahitvalue();
            metaHitVlaue->set_key(it2->first);
            metaHitVlaue->set_value(it2->second);
        }
    }
}

void PBResultFormatter::formatMeta(const ResultPtr &resultPtr, PBResult &pbResult) {
    const MetaMap& metaMap = resultPtr->getMetaMap();
    for (MetaMap::const_iterator it = metaMap.begin(); it != metaMap.end(); ++it) {
        const Meta &meta = it->second;
        if (meta.empty()) {
            continue;
        }
        PBMetaMap *pbMetaMap = pbResult.add_metamap();
        pbMetaMap->set_metakey(it->first);
        for (Meta::const_iterator it2 = meta.begin(); it2 != meta.end(); ++it2) {
            PBKVPair *metaVlaue = pbMetaMap->add_metavalue();
            metaVlaue->set_key(it2->first);
            metaVlaue->set_value(it2->second);
        }
    }
}

void PBResultFormatter::formatHit(const HitPtr &hitPtr, PBHit *pbHit,
                                  bool isIndependentQuery)
{
    pbHit->set_clustername(hitPtr->getClusterName());
    pbHit->set_hashid(hitPtr->getHashId());
    pbHit->set_docid(hitPtr->getDocId());
    pbHit->set_searcherip(hitPtr->getIp());

    if (isIndependentQuery) {
        GlobalIdentifier gid = hitPtr->getGlobalIdentifier();
        pbHit->set_fullindexversion(gid.getFullIndexVersion());
        pbHit->set_indexversion(gid.getIndexVersion());
        primarykey_t primaryKey = gid.getPrimaryKey();
        pbHit->set_pkhigher(primaryKey.value[0]);
        pbHit->set_pklower(primaryKey.value[1]);
    }
    const string &rawPk = hitPtr->getRawPk();
    if (!rawPk.empty()) {
        pbHit->set_rawpk(rawPk);
    }
    formatSummary(hitPtr, pbHit);
    formatProperty(hitPtr, pbHit);
    formatAttributes(hitPtr, pbHit);
    formatVariableValues(hitPtr, pbHit);

    for (uint32_t j = 0; j < hitPtr->getSortExprCount(); ++j) {
        pbHit->add_sortvalues(hitPtr->getSortExprValue(j));
    }

    Tracer *tracer = hitPtr->getTracer();
    if (tracer){
        pbHit->set_tracer(tracer->getTraceInfo());
    }
}

void PBResultFormatter::formatSummary(const HitPtr &hitPtr,
                                      PBHit *pbHit)
{
    SummaryHit *summaryHit = hitPtr->getSummaryHit();
    if (!summaryHit) {
        return;
    }
    const config::HitSummarySchema *hitSummarySchema =
        summaryHit->getHitSummarySchema();
    bool needFillSummaryBytes = false;
    if (_option & SUMMARY_IN_BYTES) {
        _dataBuffer.clear();
        needFillSummaryBytes = true;
    }
    size_t summaryFieldCount = summaryHit->getFieldCount();
    for (size_t i = 0; i < summaryFieldCount; ++i) {
        const string &fieldName =
            hitSummarySchema->getSummaryFieldInfo(i)->fieldName;
        const ConstString *str = summaryHit->getFieldValue(i);
        if (!str) {
            continue;
        }
        if (needFillSummaryBytes) {
            _dataBuffer.write(fieldName);
            _dataBuffer.write(*str);
        } else {
            PBKVPair *summary = pbHit->add_summary();
            summary->set_key(fieldName);
            summary->set_value(string(str->data(), str->size()));
        }
    }

    if (needFillSummaryBytes) {
        pbHit->set_summarybytes(_dataBuffer.getData(), _dataBuffer.getDataLen());
    }
}

void PBResultFormatter::formatProperty(const HitPtr &hitPtr,
                                       PBHit *pbHit)
{
    const PropertyMap& propertyMap = hitPtr->getPropertyMap();
    for (PropertyMap::const_iterator it = propertyMap.begin();
         it != propertyMap.end(); ++it)
    {
        PBKVPair *property = pbHit->add_property();
        property->set_key(it->first);
        property->set_value(it->second);
    }
}

void PBResultFormatter::formatAttributes(const HitPtr &hitPtr,
        PBHit *pbHit)
{
    const AttributeMap& attributeMap = hitPtr->getAttributeMap();
    for (AttributeMap::const_iterator it = attributeMap.begin();
         it != attributeMap.end(); ++it)
    {
        PBAttrKVPair *attribute = pbHit->add_attributes();
        fillTypedValue(it->first, it->second.get(), attribute);
    }
}

void PBResultFormatter::formatVariableValues(const HitPtr &hitPtr,
        PBHit *pbHit)
{
    const AttributeMap& variableValueMap = hitPtr->getVariableValueMap();
    for (AttributeMap::const_iterator it = variableValueMap.begin();
         it != variableValueMap.end(); ++it)
    {
        PBAttrKVPair *variableValue = pbHit->add_variablevalues();
        fillTypedValue(it->first, it->second.get(), variableValue);
    }
}


void PBResultFormatter::formatAggregateResults(const ResultPtr &resultPtr,
        PBResult &pbResult)
{
    const AggregateResults& aggRes = resultPtr->getAggregateResults();
    for (AggregateResults::const_iterator it = aggRes.begin();
         it != aggRes.end(); it++)
    {
        PBAggregateResults *pbAggregateResults = pbResult.add_aggresults();
        AggregateResultPtr aggResultPtr = *it;
        assert(*it);
        pbAggregateResults->set_aggregatekey(aggResultPtr->getGroupExprStr());
        formatAggregateResult(aggResultPtr, *pbAggregateResults);
    }
}

void PBResultFormatter::formatAggregateResult(
        const AggregateResultPtr &aggResultPtr,
        PBAggregateResults &pbAggregateResults)
{
    const auto &aggValueMap = aggResultPtr->getAggExprValueMap();
    const auto &allocatorPtr = aggResultPtr->getMatchDocAllocator();
    assert(allocatorPtr);
    auto referenceVec = allocatorPtr->getAllNeedSerializeReferences(SL_QRS);
    uint32_t aggFunCount = aggResultPtr->getAggFunCount();
    assert(aggFunCount + 1 == referenceVec.size());
    AggregateResult::AggExprValueMap::const_iterator it;
    for (it  = aggValueMap.begin(); it != aggValueMap.end(); ++it)
    {
        PBAggregateValue *pbAggregateValue =
            pbAggregateResults.add_aggregatevalue();

        pbAggregateValue->set_groupvalue(it->first);
        const auto aggFunResults = it->second;
        for (uint32_t i=1; i<=aggFunCount; i++) {
            PBKVPair *funNameResultPair =
                pbAggregateValue->add_funnameresultpair();
            funNameResultPair->set_key(aggResultPtr->getAggFunName(i-1));
            funNameResultPair->set_value(referenceVec[i]->toString(
                            aggFunResults));
        }
    }
}

void PBResultFormatter::formatErrorResult(const ResultPtr &resultPtr,
        PBResult &pbResult)
{
    MultiErrorResultPtr &multiErrorResult = resultPtr->getMultiErrorResult();
    const ErrorResults &errorResults = multiErrorResult->getErrorResults();
    for (ErrorResults::const_iterator it = errorResults.begin();
         it != errorResults.end(); ++it)
    {
        PBErrorResult *pbErrorResult = pbResult.add_errorresults();
        pbErrorResult->set_errorcode(it->getErrorCode());
        pbErrorResult->set_errordescription(it->getErrorDescription());
        pbErrorResult->set_partitionid(it->getPartitionID());
        pbErrorResult->set_hostname(it->getHostName());
    }
}

void PBResultFormatter::formatTotalTime(const ResultPtr &resultPtr,
                                        PBResult &pbResult)
{
    pbResult.set_totaltime(resultPtr->getTotalTime());
}

void PBResultFormatter::formatRequestTracer(const ResultPtr &resultPtr,
        PBResult &pbResult)
{
    Tracer *tracer = resultPtr->getTracer();
    if (tracer){
        pbResult.set_tracer(tracer->getTraceInfo());
    }
}

void PBResultFormatter::fillTypedValue(const string &key,
                                       const AttributeItem *attrItem,
                                       PBAttrKVPair *value)
{
    value->set_key(key);
    auto vt = attrItem->getType();
    switch (vt) {
        FILL_INT_ATTRIBUTE(vt_bool);
        FILL_INT_ATTRIBUTE(vt_int8);
        FILL_INT_ATTRIBUTE(vt_int16);
        FILL_INT_ATTRIBUTE(vt_int32);
        FILL_INT_ATTRIBUTE(vt_int64);
        FILL_INT_ATTRIBUTE(vt_uint8);
        FILL_INT_ATTRIBUTE(vt_uint16);
        FILL_INT_ATTRIBUTE(vt_uint32);
        FILL_INT_ATTRIBUTE(vt_uint64);

        FILL_DOUBLE_ATTRIBUTE(vt_float);
        FILL_DOUBLE_ATTRIBUTE(vt_double);

    case vt_string:
    {
        FILL_VALUE(string, bytesvalue);
        break;
    }
    case vt_hash_128:
    {
        assert(!attrItem->isMultiValue());
        const AttributeItemTyped<primarykey_t> *attrItemTyped =
            static_cast<const AttributeItemTyped<primarykey_t> *>(attrItem);
        assert(attrItemTyped);
        const primarykey_t &v = attrItemTyped->getItem();
        value->add_bytesvalue(StringUtil::toString(v));
        break;
    }
    default:
        assert(false);
        break;
    }
}
#undef READ_AND_TO_DOUBLE
#undef FILL_ATTRIBUTE
#undef FILL_INT_ATTRIBUTE
#undef FILL_DOUBLE_ATTRIBUTE
#undef FILL_VALUE

END_HA3_NAMESPACE(common);
