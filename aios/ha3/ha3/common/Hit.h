#ifndef ISEARCH_HIT_H
#define ISEARCH_HIT_H

#include <map>
#include <string>
#include <vector>
#include <sstream>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/AttributeItem.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/SummaryHit.h>

BEGIN_HA3_NAMESPACE(common);
class Hit;
typedef std::tr1::shared_ptr<Hit> HitPtr;

typedef std::string PrimaryKey;
typedef std::string DistinctExprValue;
typedef std::map<std::string, std::string> PropertyMap;
typedef std::map<std::string, std::string> SummaryMap;
typedef std::map<std::string, AttributeItemPtr> AttributeMap;

struct SortExprValue
{
    std::string valueStr;
    VariableType vt;

    SortExprValue() {
        vt = vt_unknown;
    }

    SortExprValue(const std::string &value, VariableType vt_)
        : valueStr(value)
        , vt(vt_)
    {}

    bool operator < (const SortExprValue &other) const {
        if (vt == vt_string) {
            return valueStr < other.valueStr;
        }
        double v1 = autil::StringUtil::fromString<double>(valueStr);
        double v2 = autil::StringUtil::fromString<double>(other.valueStr);
        return v1 < v2;
    }

    bool operator == (const SortExprValue &other) const {
        return valueStr == other.valueStr && vt == other.vt;
    }

    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(valueStr);
        dataBuffer.write(vt);
    }

    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(valueStr);
        dataBuffer.read(vt);
    }
};
typedef std::vector<SortExprValue> SortExprValues;

class Hit
{
public:
    Hit(docid_t docid = INVALID_DOCID, hashid_t hashid = 0,
        const std::string& clusterName = "", double sortValue = 0.0);
    ~Hit();
public:
    docid_t getDocId() const { return _globalId.getDocId(); }
    void setDocId(docid_t docId) { _globalId.setDocId(docId); }

    versionid_t getIndexVersion() const { return _globalId.getIndexVersion(); }
    void setIndexVersion(versionid_t versionId) { _globalId.setIndexVersion(versionId); }

    hashid_t getHashId() const { return _globalId.getHashId(); }
    void setHashId(hashid_t hashId) { _globalId.setHashId(hashId); }

    void setFullIndexVersion(FullIndexVersion indexVersion) {
        _globalId.setFullIndexVersion(indexVersion);
    }
    FullIndexVersion getFullIndexVersion() const {
        return _globalId.getFullIndexVersion();
    }
    clusterid_t getClusterId() const {
        return _globalId.getClusterId();
    }
    void setClusterId(clusterid_t clusterId) {
        _globalId.setClusterId(clusterId);
    }
    uint32_t getPosition() const { return _globalId.getPosition(); }
    void setPosition(uint32_t pos) { _globalId.setPosition(pos); }

    const GlobalIdentifier &getGlobalIdentifier() const { return _globalId;}
    GlobalIdentifier &getGlobalIdentifier() { return _globalId;}
    void setGlobalIdentifier(const GlobalIdentifier &globalidentifier) {
        _globalId = globalidentifier;
    }

    uint32_t getIp() const  {
        return _globalId.getIp();
    }
    void setIp(uint32_t ip) {
        _globalId.setIp(ip);
    }

    bool hasPrimaryKey() const {return _hasPrimaryKey;}
    void setHasPrimaryKeyFlag(bool flag) {_hasPrimaryKey = flag;}

    primarykey_t getPrimaryKey() const { return _globalId.getPrimaryKey(); }
    void setPrimaryKey(primarykey_t primaryKey) {
        _globalId.setPrimaryKey(primaryKey);
        setHasPrimaryKeyFlag(true);
    }

    void toGid(std::stringstream& ss) const;

    const std::string &getClusterName() const { return _clusterName; }
    void setClusterName(const std::string &clusterName) {
        _clusterName = clusterName;
    }
    void setSortValue(double sortValue) {_sortValue = sortValue;}
    double getSortValue() const {return _sortValue;}
    void stealSummary(Hit &other);

    void setSummaryValue(const std::string &fieldName, const std::string &fieldValue);
    std::string getSummaryValue(const std::string &summaryName) const;

    void setSummaryHit(SummaryHit *summaryHit) {
        if (_summaryHit) {
            delete _summaryHit;
        }
        _summaryHit = summaryHit;
    }
    SummaryHit *getSummaryHit() {
        return _summaryHit;
    }
    bool hasSummary() const {
        return _summaryHit != NULL;
    }

    const std::string& getRawPk() const {
        return _rawPk;
    }
    void setRawPk(const std::string &rawPk) {
        _rawPk = rawPk;
    }

    uint32_t getSortExprCount() const {return _sortExprValues.size();}

    const std::string& getSortExprValue(uint32_t pos) const {
        assert(pos < _sortExprValues.size());
        return _sortExprValues[pos].valueStr;
    }
    VariableType getSortExprValueType(uint32_t pos) const {
        assert(pos < _sortExprValues.size());
        return _sortExprValues[pos].vt;
    }
    const SortExprValue& getSortExprValueStructure(uint32_t pos) const {
        assert(pos < _sortExprValues.size());
        return _sortExprValues[pos];
    }
    void setSortExprValue(const SortExprValue &exprValue, uint32_t pos) {
        if (pos >= _sortExprValues.size()) {
            return;
        }
        _sortExprValues[pos] = exprValue;
    }
    void addSortExprValue(const std::string& sortExprValue,
                          VariableType vt = vt_double)
    {
        _sortExprValues.push_back(SortExprValue(sortExprValue, vt));
    }

    /** the Property interface. */
    PropertyMap& getPropertyMap();
    const std::string& getProperty(const std::string &propName) const;
    void insertProperty(const std::string &propName,
                        const std::string &propValue);

    std::string toString() const;

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool *pool);
    void setTracer(Tracer *tracer);
    void setTracerWithoutCreate(Tracer *tracer) {
        if (_tracer) {
            delete _tracer;
        }
        _tracer = tracer;
    }

    Tracer *getTracer(){
        return _tracer;
    }
    std::string getAttributeString(const std::string &attrName);
    inline bool addAttribute(const std::string &attrName, const AttributeItemPtr &v) {
        return ERROR_NONE == setAttributeItem(attrName, v, _attributes);
    }
    inline ErrorCode addVariableValue(const std::string &varName, const AttributeItemPtr &v) {
        return setAttributeItem(varName, v, _variableValueMap);
    }

    AttributeMap& getVariableValueMap() {
        return _variableValueMap;
    }

    void copyAttribute(HitPtr &hitPtr);
    AttributeMap& getAttributeMap();
    AttributeItemPtr getAttribute(const std::string &attrName) const;

    template<typename T>
    inline bool getAttribute(const std::string &attrName, T& value) const {
        return ERROR_NONE == getValue(attrName, value, _attributes);
    }

    template<typename T>
    inline bool addAttribute(const std::string &attrName, const T &value) {
        return ERROR_NONE == setValue(attrName, value, _attributes);
    }

    template<typename T>
    inline ErrorCode getVariableValue(const std::string &variableName, T& value) const{
        return getValue(variableName, value, _variableValueMap);
    }

    template<typename T>
    inline ErrorCode addVariableValue(const std::string &variableName, const T& value) {
        return setValue(variableName, value, _variableValueMap);
    }
    bool lessThan (const HitPtr &hitPtr) const;

public:
    uint32_t getSummaryCount() const;

private:
    template<typename T>
    inline ErrorCode getValue(const std::string &variableName,
                              T& value, const AttributeMap &valueMap) const
    {
        AttributeMap::const_iterator it = valueMap.find(variableName);
        if (it == valueMap.end()){
            return ERROR_VARIABLE_NAME_NOT_EXIST;
        }

        AttributeItemPtr attrItemPtr = it->second;
        if (suez::turing::TypeHaInfoTraits<T>::VARIABLE_TYPE != attrItemPtr->getType()
            || suez::turing::TypeHaInfoTraits<T>::IS_MULTI != attrItemPtr->isMultiValue())
        {
            return ERROR_VARIABLE_TYPE_NOT_MATCH;
        }

        AttributeItemTyped<T> *attrItemTyped = static_cast<AttributeItemTyped<T>*>(attrItemPtr.get());
        value = attrItemTyped->getItem();
        return ERROR_NONE;
    }

    template<typename T>
    inline ErrorCode setValue(const std::string &variableName,
                              const T &value, AttributeMap &valueMap) const
    {

        AttributeMap::const_iterator it = valueMap.find(variableName);
        if (it != valueMap.end()){
            return ERROR_VARIABLE_NAME_ALREADY_EXIST;
        }

        AttributeItemTyped<T>* pAttrItemTyped = new AttributeItemTyped<T>(value);
        AttributeItemPtr attrItemPtr(pAttrItemTyped);
        valueMap[variableName] = attrItemPtr;
        return ERROR_NONE;
    }

    inline ErrorCode setAttributeItem(const std::string &name,
            const AttributeItemPtr &v,
            AttributeMap &valueMap) const
    {
        AttributeMap::const_iterator it = valueMap.find(name);
        if (it != valueMap.end()){
            return ERROR_VARIABLE_NAME_ALREADY_EXIST;
        }
        valueMap[name] = v;
        return ERROR_NONE;
    }
public:
    static const std::string NULL_STRING;
private:
    GlobalIdentifier _globalId;
    bool _hasPrimaryKey;
    std::string _clusterName;
    AttributeMap _attributes;
    AttributeMap _variableValueMap;
    double _sortValue;
    SortExprValues _sortExprValues;
    SummaryHit *_summaryHit;
    PropertyMap _properties;
    Tracer *_tracer;
    std::string _rawPk;

private:
    enum class SummaryTraceStatus : int8_t {
        NO_SUMMARY_HIT = 0x0,
        HAS_SUMMARY_HIT = 0x1,
        HAS_SUMMARY_HIT_TRACE = 0x2
    };
    const SummaryTraceStatus getSummaryTraceStatus() const {
        if (_summaryHit != NULL) {
            if (_tracer == NULL || _tracer->getTraceInfo().empty()) {
                return SummaryTraceStatus::HAS_SUMMARY_HIT;
            }
            return SummaryTraceStatus::HAS_SUMMARY_HIT_TRACE;
        }
        return SummaryTraceStatus::NO_SUMMARY_HIT;
    }
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_HIT_H
