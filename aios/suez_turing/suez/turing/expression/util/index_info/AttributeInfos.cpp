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
#include "suez/turing/expression/util/AttributeInfos.h"

#include <assert.h>
#include <utility>

#include "alog/Logger.h"

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, AttributeInfos);

AttributeInfo::AttributeInfo() : _sortFlag(false), _multiValueFlag(false), _isSubDocAttribute(false) {}

AttributeInfo::AttributeInfo(const std::string &name,
                             const FieldInfo &fieldInfo,
                             matchdoc::ValueType vt,
                             bool sortFlag,
                             bool multiValueFlag,
                             bool isSubDocAttribute)
    : _attrName(name)
    , _fieldInfo(fieldInfo)
    , _vt(vt)
    , _sortFlag(sortFlag)
    , _multiValueFlag(multiValueFlag)
    , _isSubDocAttribute(isSubDocAttribute) {}

AttributeInfo::~AttributeInfo() {}

/////////////////////////////////////////////////////////////////////
// class AttributeInfos
/////////////////////////////////////////////////////////////////////

AttributeInfos::AttributeInfos() {}

AttributeInfos::AttributeInfos(const AttributeInfos &attrInfos) {

    for (AttrVector::const_iterator it = attrInfos._attrInfos.begin(); it != attrInfos._attrInfos.end(); it++) {
        addAttributeInfo(new AttributeInfo(*(*it)));
    }
}

AttributeInfos::~AttributeInfos() {
    for (AttrVector::iterator it = _attrInfos.begin(); it != _attrInfos.end(); it++) {
        delete *it;
    }
    _attrInfos.clear();
    _attrName2IdMap.clear();
}

const AttributeInfo *AttributeInfos::getAttributeInfo(const std::string &attrName) const {
    NameMap::const_iterator it = _attrName2IdMap.find(attrName);
    if (it == _attrName2IdMap.end()) {
        AUTIL_LOG(TRACE2, "NOT find the attrname: %s", attrName.c_str());
        return NULL;
    }
    assert(it->second < _attrInfos.size());
    return _attrInfos[it->second];
}

void AttributeInfos::addAttributeInfo(AttributeInfo *attrInfo) {
    NameMap::iterator it = _attrName2IdMap.find(attrInfo->getAttrName());
    if (it != _attrName2IdMap.end()) {
        AUTIL_LOG(WARN, "Duplicated attr: %s", attrInfo->getAttrName().c_str());
        delete attrInfo;
        return;
    }
    AUTIL_LOG(DEBUG, "add attribute: %s", attrInfo->getAttrName().c_str());
    _attrName2IdMap.insert(make_pair(attrInfo->getAttrName(), _attrInfos.size()));
    _attrInfos.emplace_back(attrInfo);
}

const AttributeInfo *AttributeInfos::getAttributeInfo(uint32_t idx) const {
    return idx < _attrInfos.size() ? _attrInfos[idx] : NULL;
}

void AttributeInfos::stealAttributeInfos(AttrVector &attrVec) {
    attrVec = _attrInfos;
    _attrInfos.clear();
    _attrName2IdMap.clear();
}

} // namespace turing
} // namespace suez
