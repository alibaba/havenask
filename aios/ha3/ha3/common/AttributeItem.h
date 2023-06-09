/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <assert.h>
#include <stddef.h>
#include <map>
#include <sstream>
#include <string>
#include <type_traits>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/util/XMLFormatUtil.h"

namespace isearch {
namespace rank {
class DistinctInfo;

} // namespace rank
} // namespace isearch

namespace autil {
template<> 
inline std::string StringUtil::toString<isearch::rank::DistinctInfo>(
        const isearch::rank::DistinctInfo &distInfo)
{
    return "";
}
}

namespace isearch {
namespace common {

class AttributeItem;

typedef std::shared_ptr<AttributeItem> AttributeItemPtr;
typedef std::map<std::string, AttributeItemPtr> AttributeItemMap;
typedef std::shared_ptr<AttributeItemMap> AttributeItemMapPtr;

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
    AUTIL_LOG_DECLARE();
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

} // namespace common
} // namespace isearch

namespace autil {

template<>
inline void DataBuffer::write<isearch::common::AttributeItem>(isearch::common::AttributeItem const * const &p) {
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
inline void DataBuffer::read<isearch::common::AttributeItem>(isearch::common::AttributeItem* &p) {
    bool isNull;
    read(isNull);
    if (isNull) {
        VariableType type;
        bool isMulti;
        read(type);
        read(isMulti);
        p = isearch::common::AttributeItem::createAttributeItem(type, isMulti);
        read(*p);
    } else {
        p = NULL;
    }
}

}

