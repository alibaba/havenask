#include <ha3/summary/SummaryExtractorProvider.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryExtractorProvider);

SummaryExtractorProvider::SummaryExtractorProvider(const SummaryQueryInfo *queryInfo,
        const FieldSummaryConfigVec *fieldSummaryConfigVec,
        const common::Request *request,
        AttributeExpressionCreator *attrExprCreator,
        config::HitSummarySchema *hitSummarySchema,
        search::SearchCommonResource& resource)
    : _queryInfo(queryInfo)
    , _fieldSummaryConfigVec(fieldSummaryConfigVec)
    , _request(request)
    , _attrExprCreator(attrExprCreator)
    , _hitSummarySchema(hitSummarySchema)
    , _matchDocAllocator(resource.matchDocAllocator.get())
    , _tracer(resource.tracer)
    , _summaryProvider(request->getConfigClause()->getKVPairs(), _tracer)
    , _cavaAllocator(resource.cavaAllocator)
{
}

SummaryExtractorProvider::~SummaryExtractorProvider() {
}

bool SummaryExtractorProvider::fillAttributeToSummary(const string& attrName) {
    assert(_hitSummarySchema);
    assert(_attrExprCreator);
    if (_hitSummarySchema->getSummaryFieldInfo(attrName)) {
        return true;
    }

    AttributeExpression *attrExpr =
        _attrExprCreator->createAtomicExpr(attrName);
    if (!attrExpr || !attrExpr->allocate(_matchDocAllocator)) {
        HA3_LOG(WARN, "create attribute expression[%s] failed.",
                attrName.c_str());
        return false;
    }
    FieldType ft = ft_unknown;
    bool isMultiValue = attrExpr->isMultiValue();
    auto vt = attrExpr->getType();

#define FIELD_TYPE_CONVERT_HELPER(vt_type)                              \
    case vt_type:                                                       \
        ft = VariableType2FieldTypeTraits<vt_type>::FIELD_TYPE;         \
        break

    switch (vt) {
        FIELD_TYPE_CONVERT_HELPER(vt_int8);
        FIELD_TYPE_CONVERT_HELPER(vt_int16);
        FIELD_TYPE_CONVERT_HELPER(vt_int32);
        FIELD_TYPE_CONVERT_HELPER(vt_int64);

        FIELD_TYPE_CONVERT_HELPER(vt_uint8);
        FIELD_TYPE_CONVERT_HELPER(vt_uint16);
        FIELD_TYPE_CONVERT_HELPER(vt_uint32);
        FIELD_TYPE_CONVERT_HELPER(vt_uint64);

        FIELD_TYPE_CONVERT_HELPER(vt_float);
        FIELD_TYPE_CONVERT_HELPER(vt_double);
        FIELD_TYPE_CONVERT_HELPER(vt_string);
        FIELD_TYPE_CONVERT_HELPER(vt_hash_128);
    default:
        HA3_LOG(TRACE3, "UNKNOWN Type: %d", (int32_t)vt);
        break;
    }
#undef FIELD_TYPE_CONVERT_HELPER

    summaryfieldid_t fieldId = _hitSummarySchema->declareSummaryField(attrName,
            ft, isMultiValue);
    if (INVALID_SUMMARYFIELDID != fieldId) {
        _attributes[fieldId] = attrExpr;
    } else {
        return false;
    }

    return true;
}

END_HA3_NAMESPACE(summary);
