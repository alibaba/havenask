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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "indexlib/indexlib.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/TypeTransformer.h"

namespace suez {
namespace turing {

class AttributeInfo {
public:
    AttributeInfo();
    AttributeInfo(const std::string &name,
                  const FieldInfo &fieldInfo,
                  matchdoc::ValueType vt,
                  bool sortFlag,
                  bool multiValueFlag,
                  bool isSubDocAttribute);
    ~AttributeInfo();

public:
    const std::string &getAttrName() const { return _attrName; }
    void setAttrName(const std::string &name) { _attrName = name; }
    bool getSortFlag() const { return _sortFlag; }
    void setSortFlag(bool flag) { _sortFlag = flag; }
    bool getMultiValueFlag() const { return _multiValueFlag; }
    void setMultiValueFlag(bool flag) { _multiValueFlag = flag; }
    FieldType getFieldType() const { return _fieldInfo.fieldType; }
    void setFieldInfo(const FieldInfo &fieldInfo) { _fieldInfo = fieldInfo; }
    const FieldInfo &getFieldInfo() const { return _fieldInfo; }
    bool getSubDocAttributeFlag() const { return _isSubDocAttribute; }
    void setSubDocAttributeFlag(bool flag) { _isSubDocAttribute = flag; }
    matchdoc::ValueType getValueType() const {
        if (_vt == matchdoc::ValueType()) {
            matchdoc::ValueType vt;
            vt.setBuiltin();
            vt.setBuiltinType(TypeTransformer::transform(getFieldType()));
            vt.setStdType(false);
            vt.setMultiValue(getMultiValueFlag());
            return vt;
        }
        return _vt;
    }
    void setValueType(matchdoc::ValueType vt) { _vt = vt; }

private:
    std::string _attrName;
    FieldInfo _fieldInfo;
    matchdoc::ValueType _vt;
    bool _sortFlag;
    bool _multiValueFlag;
    bool _isSubDocAttribute;

private:
    AUTIL_LOG_DECLARE();
};

class AttributeInfos {
public:
    typedef std::vector<AttributeInfo *> AttrVector;
    typedef std::unordered_map<std::string, size_t> NameMap;
    typedef AttrVector::iterator Iterator;
    typedef AttrVector::const_iterator ConstIterator;

public:
    AttributeInfos();
    AttributeInfos(const AttributeInfos &attrInfos);
    ~AttributeInfos();

public:
    const AttributeInfo *getAttributeInfo(const std::string &attrName) const;
    const AttributeInfo *getAttributeInfo(uint32_t idx) const;
    void addAttributeInfo(AttributeInfo *attrInfo);
    const AttrVector &getAttrbuteInfoVector() const { return _attrInfos; }
    uint32_t getAttributeCount() const { return (uint32_t)_attrInfos.size(); }
    Iterator begin() { return _attrInfos.begin(); }
    Iterator end() { return _attrInfos.end(); }
    void stealAttributeInfos(AttrVector &attrVec);

private:
    AttrVector _attrInfos;
    NameMap _attrName2IdMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
