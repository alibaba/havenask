#ifndef ISEARCH_ATTRIBUTEITEM_H
#define ISEARCH_ATTRIBUTEITEM_H

#include <vector>
#include <sstream>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/XMLFormatUtil.h>
#include <ha3/util/Serialize.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <autil/StringUtil.h>
#include <matchdoc/Trait.h>

BEGIN_HA3_NAMESPACE(rank);
class DistinctInfo;
END_HA3_NAMESPACE(rank);

namespace autil {
template<> 
inline std::string StringUtil::toString<HA3_NS(rank)::DistinctInfo>(
        const HA3_NS(rank)::DistinctInfo &distInfo)
{
    return "";
}
}

BEGIN_HA3_NAMESPACE(common);

class AttributeItem;
HA3_TYPEDEF_PTR(AttributeItem);
typedef std::map<std::string, AttributeItemPtr> AttributeItemMap;
HA3_TYPEDEF_PTR(AttributeItemMap);

class AttributeItem
{
public:
    AttributeItem() {}
    virtual ~AttributeItem() {}
public:
    virtual VariableType getType() const = 0;
    virtual bool isMultiValue() const = 0;
    virtual std::string toString() = 0;
    virtual void toXMLString(std::stringstream &oss) = 0;

    virtual void serialize(autil::DataBuffer &dataBuffer) const = 0;
    virtual void deserialize(autil::DataBuffer &dataBuffer) = 0;

    static AttributeItem *createAttributeItem(VariableType variableType,
            bool isMulti);
private:
    HA3_LOG_DECLARE();
};

template<typename T>
class AttributeItemBase : public AttributeItem
{
public:
    AttributeItemBase() {}
    AttributeItemBase(const T& item) : _item(item) {}
    ~AttributeItemBase() {}
public:
    void setItem(const T &item) {
        _item = item;
    }
    const T &getItem() const {
        return _item;
    }
    T getItem() {
        return _item;
    }
    T *getPointer() {
        return &_item;
    }
    VariableType getType() const override {
        return suez::turing::TypeHaInfoTraits<T>::VARIABLE_TYPE;
    }
    bool isMultiValue() const override {
        return suez::turing::TypeHaInfoTraits<T>::IS_MULTI;
    }
protected:
    T _item;
};

template<typename T, bool =
         !std::is_same<typename matchdoc::ToStringTypeTraits<T>::Type,
                       matchdoc::ToStringType>::value>
class AttributeItemTyped;

template<typename T> 
class AttributeItemTyped<T, false> : public AttributeItemBase<T>
{
public:
    AttributeItemTyped() {}
    AttributeItemTyped(const T& item) : AttributeItemBase<T>(item) {}
    ~AttributeItemTyped() {}
public:
    std::string toString() override {
        assert(false);
        return "";
    }

    void toXMLString(std::stringstream &ss) override {
        assert(false);
    }

    void serialize(autil::DataBuffer &dataBuffer) const override {
        assert(false);
    }
    
    void deserialize(autil::DataBuffer &dataBuffer) override {
        assert(false);
    }
};

template<typename T> 
class AttributeItemTyped<T, true> : public AttributeItemBase<T>
{
public:
    AttributeItemTyped() {}
    AttributeItemTyped(const T& item) : AttributeItemBase<T>(item) {}
    ~AttributeItemTyped() {}
public:
    using AttributeItemBase<T>::_item;
    using AttributeItemBase<T>::getType;
    using AttributeItemBase<T>::isMultiValue;
    std::string toString() override {
        return autil::StringUtil::toString(_item);
    }
    void toXMLString(std::stringstream &ss) override {
        util::XMLFormatUtil::toXMLString(_item, ss);
    }
    void serialize(autil::DataBuffer &dataBuffer) const override {
        dataBuffer.write(_item);
    }
    void deserialize(autil::DataBuffer &dataBuffer) override {
        dataBuffer.read(_item);
    }
};

END_HA3_NAMESPACE(common);

AUTIL_BEGIN_NAMESPACE(autil);

template<>
inline void DataBuffer::write<HA3_NS(common)::AttributeItem>(HA3_NS(common)::AttributeItem const * const &p) {
    bool isNull = p;
    write(isNull);
    if (isNull) {
        auto type = p->getType();
        bool isMulti = p->isMultiValue();
        write(type);
        write(isMulti);
        write(*p);
    }
}

template<> 
inline void DataBuffer::read<HA3_NS(common)::AttributeItem>(HA3_NS(common)::AttributeItem* &p) {
    bool isNull;
    read(isNull);
    if (isNull) {
        VariableType type;
        bool isMulti;
        read(type);
        read(isMulti);
        p = HA3_NS(common)::AttributeItem::createAttributeItem(type, isMulti);
        read(*p);
    } else {
        p = NULL;
    }
}

AUTIL_END_NAMESPACE(autil);

#endif //ISEARCH_ATTRIBUTEITEM_H
