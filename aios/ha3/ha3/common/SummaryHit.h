#ifndef ISEARCH_SUMMARYHIT_H
#define ISEARCH_SUMMARYHIT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/document/index_document/normal_document/search_summary_document.h>
#include <ha3/config/HitSummarySchema.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/Tracer.h>

BEGIN_HA3_NAMESPACE(common);

class SummaryHit
{
public:
    SummaryHit(config::HitSummarySchema *hitSummarySchema,
               autil::mem_pool::Pool *pool, common::Tracer *tracer = NULL);
    ~SummaryHit();
private:
    SummaryHit(const SummaryHit &);
    SummaryHit& operator=(const SummaryHit &);
public:
    bool setFieldValue(int32_t summaryFieldId, const char* fieldValue,
                       size_t size, bool needCopy = true);
    void setSummaryValue(const std::string &fieldName, const std::string &fieldValue);
    void setSummaryValue(const std::string &fieldName, const char* fieldValue,
                         size_t size, bool needCopy = true);
    bool clearFieldValue(int32_t summaryFieldId);
    bool clearFieldValue(const std::string &fieldName);
    const autil::ConstString* getFieldValue(int32_t summaryFieldId) const;
    const autil::ConstString* getFieldValue(const std::string &fieldName) const;

    const config::SummaryFieldInfo *getSummaryFieldInfo(size_t pos) const {
        return _hitSummarySchema->getSummaryFieldInfo(pos);
    }
    size_t getFieldCount() const { return _hitSummarySchema->getFieldCount(); }

    char *getBuffer(size_t size) { return _summaryDoc.GetBuffer(size); }
    common::Tracer *getHitTracer() { return _tracer;}
public:
    // inner function.
    config::HitSummarySchema *getHitSummarySchema() { return _hitSummarySchema; }
    void setHitSummarySchema(config::HitSummarySchema *hitSummarySchema) { _hitSummarySchema = hitSummarySchema; }

    IE_NAMESPACE(document)::SearchSummaryDocument *getSummaryDocument() { return &_summaryDoc; }

    void summarySchemaToSignature();
    int64_t getSignature() const { return _signature; }
    void setSignature(int64_t signature) { _signature = signature; }
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    // _signature only used to locate _hitSummarySchema after deserialize.
    int64_t _signature;
    config::HitSummarySchema *_hitSummarySchema;
    IE_NAMESPACE(document)::SearchSummaryDocument _summaryDoc;
    common::Tracer *_tracer;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryHit);

////////////////////////////////////////////////////////////////////////////

inline bool SummaryHit::setFieldValue(int32_t summaryFieldId, const char* fieldValue,
                                      size_t size, bool needCopy)
{
    if (summaryFieldId >= 0 && summaryFieldId < (int32_t)_hitSummarySchema->getFieldCount()) {
        return _summaryDoc.SetFieldValue(summaryFieldId, fieldValue, size, needCopy);
    }
    return false;
}

inline bool SummaryHit::clearFieldValue(int32_t summaryFieldId) {
    if (summaryFieldId >= 0 && summaryFieldId < (int32_t)_hitSummarySchema->getFieldCount()) {
        _summaryDoc.ClearFieldValue(summaryFieldId);
        return true;
    }
    return false;
}

inline bool SummaryHit::clearFieldValue(const std::string &fieldName) {
    const config::SummaryFieldInfo *fieldInfo = _hitSummarySchema->getSummaryFieldInfo(fieldName);
    if (fieldInfo) {
        _summaryDoc.ClearFieldValue(fieldInfo->summaryFieldId);
        return true;
    }
    return false;
}

inline const autil::ConstString* SummaryHit::getFieldValue(int32_t summaryFieldId) const {
    return _summaryDoc.GetFieldValue(summaryFieldId);
}

inline const autil::ConstString* SummaryHit::getFieldValue(const std::string &fieldName) const {
    const config::SummaryFieldInfo *fieldInfo = _hitSummarySchema->getSummaryFieldInfo(fieldName);
    if (fieldInfo) {
        return _summaryDoc.GetFieldValue(fieldInfo->summaryFieldId);
    }
    return NULL;
}


END_HA3_NAMESPACE(common);

#endif //ISEARCH_SUMMARYHIT_H
