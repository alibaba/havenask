#include <ha3/common/ColumnTerm.h>

using namespace std;
using namespace autil;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(common);

HA3_LOG_SETUP(common, ColumnTerm);

ColumnTerm::ColumnTerm(const std::string &indexName)
    : _indexName(indexName)
{
}

std::string ColumnTerm::offsetsToString() const {
    stringstream ss;
    for (auto offset: _offsets) {
        ss << offset << ",";
    }
    return ss.str();
}

void ColumnTerm::serialize(const ColumnTerm *p, autil::DataBuffer &dataBuffer) {
    bool isNull = p;
    dataBuffer.write(isNull);
    if (isNull) {
        uint8_t type = (uint8_t)p->getValueType();
        dataBuffer.write(type);
        p->save(dataBuffer);
    }
}

ColumnTerm* ColumnTerm::deserialize(autil::DataBuffer &dataBuffer) {
    ColumnTerm* p = nullptr;
    bool isNull;
    dataBuffer.read(isNull);
    if (isNull) {
        uint8_t type;
        dataBuffer.read(type);
        p = HA3_NS(common)::ColumnTerm::createColumnTerm((matchdoc::BuiltinType)type);
        if (p) {
            p->load(dataBuffer);
        }
    }
    return p;
}

ColumnTerm* ColumnTerm::createColumnTerm(BuiltinType type) {
#define CASE_MACRO(bt)                                                          \
    case bt: {                                                                  \
        using T = typename MatchDocBuiltinType2CppType<bt, false>::CppType;     \
        return new ColumnTermTyped<T>();                                        \
    }

    switch(type) {
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO)
    case bt_bool:
        return new ColumnTermTyped<bool>();
    default:
        return nullptr;
    }
#undef CASE_MACRO
}

void ColumnTerm::save(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_indexName);
    dataBuffer.write(_offsets);
    doSave(dataBuffer);
}

void ColumnTerm::load(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_indexName);
    dataBuffer.read(_offsets);
    doLoad(dataBuffer);
}

template <class T>
vector<T>& ColumnTermTyped<T>::getValues() {
    return _values;
}

template <class T>
const vector<T>& ColumnTermTyped<T>::getValues() const {
    return _values;
}

template <class T>
bool ColumnTermTyped<T>::operator == (const ColumnTerm& term) const {
    if (&term == this) {
        return true;
    }
    const ColumnTermTyped<T>* p = dynamic_cast<const ColumnTermTyped<T>*>(&term);
    if (!p) {
        return false;
    }
    return (_indexName == p->_indexName)
        && (_offsets == p->_offsets)
        && (_values == p->_values)
        && (_enableCache == p->_enableCache);
}

template <class T>
ColumnTerm *ColumnTermTyped<T>::clone() const {
    return new ColumnTermTyped<T>(*this);
}

template <class T>
string ColumnTermTyped<T>::toString() const {
    stringstream ss;
    ss << "ColumnTerm:[" << offsetsToString();
    ss << "]:[" << _indexName << ":";
    for (const auto& value : _values) {
        ss << value << ",";
    }
    ss << "]";
    return ss.str();
}

template <class T>
BuiltinType ColumnTermTyped<T>::getValueType() const {
    ValueType vt = ValueTypeHelper<T>::getValueType();
    return vt.getBuiltinType();
}

template <class T>
void ColumnTermTyped<T>::doSave(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_values);
}

template <class T>
void ColumnTermTyped<T>::doLoad(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_values);
}

template class ColumnTermTyped<uint64_t>;
template class ColumnTermTyped<int64_t>;
template class ColumnTermTyped<uint32_t>;
template class ColumnTermTyped<int32_t>;
template class ColumnTermTyped<uint16_t>;
template class ColumnTermTyped<int16_t>;
template class ColumnTermTyped<uint8_t>;
template class ColumnTermTyped<int8_t>;
template class ColumnTermTyped<bool>;
template class ColumnTermTyped<float>;
template class ColumnTermTyped<double>;
template class ColumnTermTyped<MultiChar>;

END_HA3_NAMESPACE(common);
