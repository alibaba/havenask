#include <ha3/config/HitSummarySchema.h>
#include <autil/HashAlgorithm.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, HitSummarySchema);

HitSummarySchema::HitSummarySchema(const TableInfo *tableInfo)
    : _needCalcSignature (true)
    , _signature(0)
    , _incSchema(NULL)
{
    init(tableInfo);
    calcSignature();
}

HitSummarySchema::HitSummarySchema(const HitSummarySchema &other)
    : _needCalcSignature(other._needCalcSignature)
    , _signature(other._signature)
    , _summaryFieldInfoMap(other._summaryFieldInfoMap)
    , _summaryFieldInfos(other._summaryFieldInfos)
{
    if (other._incSchema) {
        _incSchema = new HitSummarySchema(*other._incSchema);
    } else {
        _incSchema = NULL;
    }
}

HitSummarySchema::~HitSummarySchema() {
    DELETE_AND_SET_NULL(_incSchema);
}

void HitSummarySchema::init(const TableInfo *tableInfo) {
    if (!tableInfo) {
        return;
    }
    const SummaryInfo *summaryInfo = tableInfo->getSummaryInfo();
    const FieldInfos *fieldInfos = tableInfo->getFieldInfos();

    if (!summaryInfo || !fieldInfos) {
        return;
    }

    size_t summaryFieldCount = summaryInfo->getFieldCount();
    _summaryFieldInfos.resize(summaryFieldCount);

    for (size_t i = 0; i < summaryFieldCount; ++i) {
        std::string fieldName = summaryInfo->getFieldName(i);
        const FieldInfo *fieldInfo = fieldInfos->getFieldInfo(fieldName.c_str());

        assert(_summaryFieldInfoMap.find(fieldName) == _summaryFieldInfoMap.end());

        _summaryFieldInfos[i].isMultiValue = fieldInfo->isMultiValue;
        _summaryFieldInfos[i].fieldType = fieldInfo->fieldType;
        _summaryFieldInfos[i].summaryFieldId = (summaryfieldid_t)i;
        _summaryFieldInfos[i].fieldName = fieldName;

        _summaryFieldInfoMap[fieldName] = i;
    }
}

HitSummarySchema *HitSummarySchema::clone() const {
    return new HitSummarySchema(*this);
}

summaryfieldid_t HitSummarySchema::declareSummaryField(
        const string &fieldName, FieldType fieldType, bool isMultiValue)
{
    if (_summaryFieldInfoMap.find(fieldName) != _summaryFieldInfoMap.end()) {
        return INVALID_SUMMARYFIELDID;
    }
    if (_incSchema == NULL) {
        _incSchema = new HitSummarySchema();
    }
    summaryfieldid_t fieldId = _incSchema->declareIncSummaryField(fieldName,
            fieldType, isMultiValue, _summaryFieldInfos.size());
    return fieldId;
}

summaryfieldid_t HitSummarySchema::declareIncSummaryField(
        const string &fieldName, FieldType fieldType, bool isMultiValue, size_t offset)
{
    if (_summaryFieldInfoMap.find(fieldName) != _summaryFieldInfoMap.end()) {
        return INVALID_SUMMARYFIELDID;
    }
    SummaryFieldInfo summaryFieldInfo;

    summaryFieldInfo.fieldName = fieldName;
    summaryFieldInfo.fieldType = fieldType;
    summaryFieldInfo.isMultiValue = isMultiValue;
    summaryFieldInfo.summaryFieldId = _summaryFieldInfos.size() + offset;
    _summaryFieldInfoMap[fieldName] = _summaryFieldInfos.size();
    _summaryFieldInfos.push_back(summaryFieldInfo);
    _needCalcSignature = true;
    return summaryFieldInfo.summaryFieldId;
}

int64_t HitSummarySchema::getSignatureNoCalc() const {
    if (_incSchema) {
        return _signature + _incSchema->getSignatureNoCalc();
    }
    return _signature;
}

int64_t HitSummarySchema::getSignature() {
    if (_needCalcSignature) {
        calcSignature();
    }
    if (_incSchema) {
        return _signature + _incSchema->getSignature();
    }
    return _signature;
}

void HitSummarySchema::calcSignature() {
    if (_incSchema) {
        _incSchema->calcSignature();
    }
    if (_summaryFieldInfos.size() == 0) {
        return;
    }
    string hashString;
    for (std::vector<SummaryFieldInfo>::const_iterator it = _summaryFieldInfos.begin();
         it != _summaryFieldInfos.end(); ++it)
    {
        hashString.append(it->fieldName);
        hashString.append(1, '#');
    }

    _signature = HashAlgorithm::hashString64(hashString.c_str());
    _signature &= 0xffffffffffffff00;
    _signature += _summaryFieldInfos.size() & 0xff;
    _needCalcSignature = false;

}

void HitSummarySchema::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_summaryFieldInfos);
    dataBuffer.write(_incSchema);
}

void HitSummarySchema::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_summaryFieldInfos);
    for (std::vector<SummaryFieldInfo>::const_iterator it = _summaryFieldInfos.begin();
         it != _summaryFieldInfos.end(); ++it)
    {
        _summaryFieldInfoMap[it->fieldName] = it->summaryFieldId;
    }
    dataBuffer.read(_incSchema);
    if (_incSchema) {
        _incSchema->updateOffset(_summaryFieldInfos.size());
    }
    calcSignature();
}

void HitSummarySchema::updateOffset(size_t offset) {
    SummaryFieldMap::iterator iter = _summaryFieldInfoMap.begin();
    for (; iter != _summaryFieldInfoMap.end(); iter++) {
        if (iter->second >= offset) {
            iter->second -= offset;
        }
    }
}
void HitSummarySchema::clearIncSchema() {
    if (_incSchema) {
        _incSchema->clearIncSchema();
        DELETE_AND_SET_NULL(_incSchema);
    }
}

size_t HitSummarySchema::getFieldCount() const {
    int32_t fieldCount = _summaryFieldInfos.size();
    if (_incSchema) {
        return fieldCount + _incSchema->getFieldCount();
    }
    return fieldCount;
}

HitSummarySchemaPool::HitSummarySchemaPool(const HitSummarySchemaPtr &base, uint32_t count) {
    _base = base;
    for (uint32_t i = 0; i < count; i++) {
        HitSummarySchemaPtr cloneSchema(_base->clone());
        _schemaVec.push_back(cloneSchema);
    }
}
HitSummarySchemaPool::~HitSummarySchemaPool() {
    _schemaVec.clear();
}


HitSummarySchemaPtr HitSummarySchemaPool::get() {
    std::lock_guard<std::mutex> guard(_lock);
    if (_schemaVec.size() > 0) {
        HitSummarySchemaPtr back = _schemaVec.back();
        _schemaVec.pop_back();
        return back;
    } else {
        HitSummarySchemaPtr cloneSchema(_base->clone());
        return cloneSchema;
    }
}

void HitSummarySchemaPool::put(const HitSummarySchemaPtr &schema) {
    schema->clearIncSchema();
    std::lock_guard<std::mutex> guard(_lock);
    _schemaVec.push_back(schema);
}

END_HA3_NAMESPACE(config);
