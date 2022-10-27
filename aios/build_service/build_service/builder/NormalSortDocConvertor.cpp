#include "build_service/builder/NormalSortDocConvertor.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

namespace build_service {
namespace builder {
BS_LOG_SETUP(builder, NormalSortDocConvertor);

NormalSortDocConvertor::NormalSortDocConvertor()
    : _dataBuffer(NULL)
{
}

NormalSortDocConvertor::~NormalSortDocConvertor() {
    DELETE_AND_SET_NULL(_dataBuffer);
}

bool NormalSortDocConvertor::init(
    const SortDescriptions &sortDesc,
    const IndexPartitionSchemaPtr &schema)
{
    _sortDesc = sortDesc;
    _attrSchema = schema->GetAttributeSchema();
    _emitKeyFuncs.resize(_sortDesc.size(), NULL);
    _fieldIds.resize(_sortDesc.size(), 0);
    for (size_t i = 0; i < _sortDesc.size(); i++) {
        _emitKeyFuncs[i] = initEmitKeyFunc(_sortDesc[i]);
        if (_emitKeyFuncs[i] == NULL) {
            string errorMsg = "init emit key function failed.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _fieldIds[i] = _attrSchema->GetAttributeConfig(_sortDesc[i].sortFieldName)->GetFieldId();
    }
    _dataBuffer = new DataBuffer();
    return true;
}

NormalSortDocConvertor::MakeEmitKeyFuncType NormalSortDocConvertor::initEmitKeyFunc(
        const SortDescription &sortDesc) {
    const string &fieldName = sortDesc.sortFieldName;
    SortPattern sp = sortDesc.sortPattern;
    AttributeConfigPtr attrConfig = _attrSchema->GetAttributeConfig(fieldName);
    if (attrConfig == NULL || !attrConfig->GetFieldConfig()->SupportSort()) {
        return NULL;
    }
    FieldType type = attrConfig->GetFieldType();
    switch (type) {
    case ft_int8:
        if (sp == sp_asc)
            return makeAscEmitKey<int8_t>;
        else
            return makeDescEmitKey<int8_t>;
        break;
    case ft_uint8:
        if (sp == sp_asc)
            return makeAscEmitKey<uint8_t>;
        else
            return makeDescEmitKey<uint8_t>;
        break;
    case ft_int16:
        if (sp == sp_asc)
            return makeAscEmitKey<int16_t>;
        else
            return makeDescEmitKey<int16_t>;
        break;
    case ft_uint16:
        if (sp == sp_asc)
            return makeAscEmitKey<uint16_t>;
        else
            return makeDescEmitKey<uint16_t>;
        break;
    case ft_integer:
        if (sp == sp_asc)
            return makeAscEmitKey<int32_t>;
        else
            return makeDescEmitKey<int32_t>;
        break;
    case ft_uint32:
        if (sp == sp_asc)
            return makeAscEmitKey<uint32_t>;
        else
            return makeDescEmitKey<uint32_t>;
        break;
    case ft_long:
        if (sp == sp_asc)
            return makeAscEmitKey<int64_t>;
        else
            return makeDescEmitKey<int64_t>;
        break;
    case ft_uint64:
        if (sp == sp_asc)
            return makeAscEmitKey<uint64_t>;
        else
            return makeDescEmitKey<uint64_t>;
        break;
    case ft_float:
        if (sp == sp_asc)
            return makeAscEmitKey<float>;
        else
            return makeDescEmitKey<float>;
        break;
    case ft_double:
        if (sp == sp_asc)
            return makeAscEmitKey<double>;
        else
            return makeDescEmitKey<double>;
        break;
    case ft_hash_64:
        if (sp == sp_asc)
            return makeAscEmitKey<uint64_t>;
        else
            return makeDescEmitKey<uint64_t>;
        break;
    default:
        return NULL;
    }
    return NULL;
}

template<typename T>
void NormalSortDocConvertor::makeDescEmitKey(const autil::ConstString &sortValueString, string &emitKey) {
    T value = 0;
    if (sortValueString.length() == sizeof(T)) {
        value = *(T *)sortValueString.data();
    }

    makeDescEmitKey<T>(value, emitKey);
}

template<typename T>
void NormalSortDocConvertor::makeAscEmitKey(const ConstString &sortValueString, string &emitKey) {
    T sortValue = 0;
    if (sortValueString.length() == sizeof(T)) {
        sortValue = *(T *)sortValueString.data();
    }
    makeAscEmitKey<T>(sortValue, emitKey);
}

}
}
