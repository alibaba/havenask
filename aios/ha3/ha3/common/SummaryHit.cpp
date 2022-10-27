#include <ha3/common/SummaryHit.h>

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(document);

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SummaryHit);

SummaryHit::SummaryHit(HitSummarySchema *hitSummarySchema,
                       Pool *pool, Tracer* tracer)
    : _hitSummarySchema(hitSummarySchema)
    , _summaryDoc(pool, hitSummarySchema ? hitSummarySchema->getFieldCount() : 0)
    , _tracer(tracer)
{
}

SummaryHit::~SummaryHit() {
}

void SummaryHit::setSummaryValue(const string &fieldName, const string &fieldValue) {
    setSummaryValue(fieldName, fieldValue.c_str(), fieldValue.size(), true);
}

void SummaryHit::setSummaryValue(const string &fieldName, const char* fieldValue,
                                 size_t size, bool needCopy)
{
    const SummaryFieldInfo *summaryInfo = _hitSummarySchema->getSummaryFieldInfo(fieldName);
    summaryfieldid_t summaryFieldId;
    if (summaryInfo) {
        summaryFieldId = summaryInfo->summaryFieldId;
    } else {
        summaryFieldId = _hitSummarySchema->declareSummaryField(fieldName);
    }
    _summaryDoc.SetFieldValue(summaryFieldId, fieldValue, size, needCopy);
}

void SummaryHit::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_signature);
    const std::vector<autil::ConstString*>& fields = _summaryDoc.GetFields();
    uint32_t size = static_cast<uint32_t>(fields.size());
    dataBuffer.write(size);
    for (uint32_t i = 0; i < size; i++) {
        autil::ConstString *p = fields[i];
        if (!p) {
            dataBuffer.write(false);
            continue;
        }
        dataBuffer.write(true);
        dataBuffer.write(static_cast<uint32_t>(p->size()));
        dataBuffer.writeBytes(p->c_str(), p->size());
    }
}

void SummaryHit::deserialize(autil::DataBuffer &dataBuffer) {
    _hitSummarySchema = NULL;

    dataBuffer.read(_signature);
    uint32_t size = 0;
    dataBuffer.read(size);
    _summaryDoc.UpdateFieldCount(size);
    for (uint32_t i = 0; i < size; ++i) {
        bool hasValue = false;
        dataBuffer.read(hasValue);
        if (!hasValue) {
            continue;
        } else {
            uint32_t strLen = 0;
            dataBuffer.read(strLen);
            const char *data = (const char*)dataBuffer.readNoCopy(strLen);
            _summaryDoc.SetFieldValue(i, data, strLen);
        }
    }
}

void SummaryHit::summarySchemaToSignature() {
    // _hitSummarySchema is NULL in proxy.
    if (_hitSummarySchema) {
        _signature = _hitSummarySchema->getSignature();
        _hitSummarySchema = NULL;
    }
}

END_HA3_NAMESPACE(common);
