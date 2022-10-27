#include <ha3/common/Hit.h>
#include <algorithm>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, Hit);

//static
const string Hit::NULL_STRING = "";

Hit::Hit(docid_t docid, hashid_t hashid, const string& clusterName, double sortValue) 
    : _clusterName(clusterName)
    , _sortValue(sortValue)
{
    setDocId(docid);
    setHashId(hashid);
    _hasPrimaryKey = false;
    _summaryHit = NULL;
    _tracer = NULL;
}

Hit::~Hit() { 
    if (_tracer) {
        delete _tracer;
        _tracer = NULL;
    }
    if (_summaryHit) {
        delete _summaryHit;
        _summaryHit = NULL;
    }
}

void Hit::stealSummary(Hit &other) {
    assert(getDocId() == other.getDocId());
    assert(getHashId() == other.getHashId());
    assert(getClusterId() == other.getClusterId());
    assert(!_summaryHit);
    _summaryHit = other._summaryHit;
    other._summaryHit = NULL;
    if (_tracer) {
        _tracer->merge(other._tracer);
    } else {
        _tracer = other._tracer;
        other._tracer = NULL;
    }
}

string Hit::toString() const {
    stringstream ss;
    ss << "docId: " << getDocId() << endl;
    ss << "hashId: " << getHashId() << endl;
    ss << "clusterId: " << getClusterId() << endl;
    ss << "position: " << getPosition() << endl;
    ss << "clusterName: " << getClusterName() << endl;
    ss << "sortValue: " << getSortValue() << endl;
    ss << "attributes: size=" << _attributes.size() << endl;
    for (AttributeMap::const_iterator it = _attributes.begin();
         it != _attributes.end(); it++){
        ss << "\t" << it->first << " : " << it->second->toString() << endl;
    }
    ss << "properties: size=" << _properties.size() << endl;
    for (PropertyMap::const_iterator it = _properties.begin();
         it != _properties.end(); it++){
        ss << "\t" << it->first << " : " << it->second << endl;
    }
    ss << "sortexprvalues: size=" << _sortExprValues.size() << endl;
    for (uint32_t i=0; i< _sortExprValues.size(); i++){
        ss << "\t[" << i << "] "<< _sortExprValues[i].valueStr << endl;
    }
    if (_tracer) {
        ss << "tracer:"<< endl;
        ss << _tracer->toXMLString() << endl;
    }
    if (!_rawPk.empty()) {
        ss << "rawPk:" << _rawPk << endl;
    }
    return ss.str();
}

uint32_t Hit::getSummaryCount() const {
    if (!_summaryHit) {
        return 0;
    }
    return _summaryHit->getFieldCount();
}

void Hit::setSummaryValue(const string &fieldName, const string &fieldValue) {
    if (_summaryHit) {
        _summaryHit->setSummaryValue(fieldName, fieldValue);
    }
}

string Hit::getSummaryValue(const string &summaryName) const {
    if (!_summaryHit) {
        return Hit::NULL_STRING;
    }
    const autil::ConstString *str = _summaryHit->getFieldValue(summaryName);
    if (!str) {
        return Hit::NULL_STRING;
    }
    return string(str->data(), str->size());
}

PropertyMap& Hit::getPropertyMap() {
    return _properties;
}

const string& Hit::getProperty(const string &propName) const {
    PropertyMap::const_iterator it = _properties.find(propName);
    if (it != _properties.end()) {
        return it->second;
    }
    return Hit::NULL_STRING;
}

void Hit::insertProperty(const string &propName, const string &propValue) {
    _properties.insert(make_pair(propName, propValue));
}

string Hit::getAttributeString(const string &attrName) {
    AttributeMap::const_iterator it = _attributes.find(attrName);
    if (it == _attributes.end()){
        return "";
    }
    return it->second->toString();
}

void Hit::copyAttribute(HitPtr &hitPtr) {
    AttributeMap &attrMap = hitPtr->getAttributeMap();
    _attributes.insert(attrMap.begin(), attrMap.end());
}

AttributeMap& Hit::getAttributeMap() {
    return _attributes;
}

AttributeItemPtr Hit::getAttribute(const string &attrName) const {
    AttributeMap::const_iterator it = _attributes.find(attrName);
    if (it != _attributes.end()){
        return it->second;
    }
    return AttributeItemPtr();
}

void Hit::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_globalId);
    dataBuffer.write(_clusterName);
    dataBuffer.write(_sortValue);
    dataBuffer.write(_attributes);
    dataBuffer.write(_sortExprValues);

    SummaryTraceStatus status = getSummaryTraceStatus();
    dataBuffer.write(status);
    if (_summaryHit) {
        _summaryHit->summarySchemaToSignature();
        // compatible for old version
        dataBuffer.write(*_summaryHit);
        if (status == SummaryTraceStatus::HAS_SUMMARY_HIT_TRACE) {
            dataBuffer.write(_tracer);
        }
    }

    dataBuffer.write(_properties);
    dataBuffer.write(_variableValueMap);
}

void Hit::deserialize(autil::DataBuffer &dataBuffer, Pool *pool) {
    dataBuffer.read(_globalId);
    dataBuffer.read(_clusterName);
    dataBuffer.read(_sortValue);
    dataBuffer.read(_attributes);
    dataBuffer.read(_sortExprValues);

    SummaryTraceStatus status = SummaryTraceStatus::NO_SUMMARY_HIT;
    dataBuffer.read(status);
    // compatible for old version
    if (status != SummaryTraceStatus::NO_SUMMARY_HIT) {
        _summaryHit = new SummaryHit(NULL, pool);
        dataBuffer.read(*_summaryHit);
        if (status == SummaryTraceStatus::HAS_SUMMARY_HIT_TRACE) {
            dataBuffer.read(_tracer);
        }
    }
    dataBuffer.read(_properties);
    dataBuffer.read(_variableValueMap);
}

void Hit::setTracer(Tracer *tracer) {
    if (_tracer == NULL) {
        _tracer = new Tracer();
        assert(_tracer);
    }
    *_tracer = *tracer;
}

void Hit::toGid(stringstream& ss) const {
    ss << _clusterName << FETCHSUMMARY_GID_SEPERATOR
       << _globalId.getFullIndexVersion() << FETCHSUMMARY_GID_SEPERATOR
       << _globalId.getIndexVersion() << FETCHSUMMARY_GID_SEPERATOR
       << _globalId.getHashId() << FETCHSUMMARY_GID_SEPERATOR
       << _globalId.getDocId() << FETCHSUMMARY_GID_SEPERATOR
       << _globalId.getPrimaryKey() << FETCHSUMMARY_GID_SEPERATOR
       << _globalId.getIp();
}

bool Hit::lessThan(const HitPtr &hitPtr) const {
    if (getSortValue() == hitPtr->getSortValue()) {
        return getGlobalIdentifier() < hitPtr->getGlobalIdentifier();
    }
    return getSortValue() < hitPtr->getSortValue();
}

END_HA3_NAMESPACE(common);
