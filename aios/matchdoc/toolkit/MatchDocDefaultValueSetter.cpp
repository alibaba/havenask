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
#include "matchdoc/toolkit/MatchDocDefaultValueSetter.h"

#include "autil/MultiValueCreator.h"
#include "autil/StringUtil.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/ValueType.h"

namespace matchdoc {

using namespace std;
using namespace autil;

AUTIL_LOG_SETUP(matchdoc, MatchDocDefaultValueSetter);

bool MatchDocDefaultValueSetter::init(const map<string, string> &defaultValues) {
    if (!createFieldSetters(defaultValues)) {
        return false;
    }

    std::map<uint32_t, char *> mountIdToMountBuffer;
    if (!initMountBuffer(mountIdToMountBuffer)) {
        AUTIL_LOG(WARN, "init mount buffer failed.");
        return false;
    }

    const MountInfoPtr &mountInfoPtr = _allocator->getMountInfo();
    for (auto &fieldSetter : _fieldSetters) {
        const string &fieldName = fieldSetter.getFieldName();
        const string &defaultValue = defaultValues.at(fieldName);
        const MountMeta *mountMeta = mountInfoPtr->get(fieldName);
        if (nullptr == mountMeta) {
            if (!fieldSetter.init(defaultValue)) {
                AUTIL_LOG(WARN, "init field default setter for  [%s] failed.", fieldName.c_str());
                return false;
            }
        } else {
            char *mountBuffer = mountIdToMountBuffer[mountMeta->mountId];
            if (!fieldSetter.init(defaultValue, mountBuffer)) {
                AUTIL_LOG(WARN, "init mounted field default setter for [%s] failed.", fieldName.c_str());
                return false;
            }
        }
    }
    return true;
}

void MatchDocDefaultValueSetter::setDefaultValues(matchdoc::MatchDoc doc) {
    for (const auto &fieldSetter : _fieldSetters) {
        fieldSetter.setDefaultValue(doc);
    }
}

bool MatchDocDefaultValueSetter::createFieldSetters(const map<string, string> &defaultValues) {
    if (defaultValues.empty()) {
        return false;
    }
    bool valid = false;
    for (const auto &nameToValue : defaultValues) {
        const string &fieldName = nameToValue.first;
        const string &defaultValue = nameToValue.second;
        ReferenceBase *ref = _allocator->findReferenceWithoutType(fieldName);
        if (nullptr == ref) {
            AUTIL_LOG(WARN, "field [%s] is not exist.", fieldName.c_str());
            return false;
        } else if (ref->getValueType().isMultiValue()) {
            AUTIL_LOG(WARN, "field [%s] with mount_multi_value is not support set default value.", fieldName.c_str());
            return false;
        }
        if (!defaultValue.empty()) {
            valid = true;
            _fieldSetters.push_back(FieldDefaultValueSetter(_pool, ref));
        }
    }
    return valid;
}

bool MatchDocDefaultValueSetter::initMountBuffer(map<uint32_t, char *> &mountIdToMountBuffer) {
    const MountInfoPtr &mountInfoPtr = _allocator->getMountInfo();
    if (nullptr == mountInfoPtr) {
        return true;
    }

    const std::unordered_map<std::string, ReferenceBase *> references = _allocator->getFastReferenceMap();
    map<uint32_t, int64_t> mountIdToLengthMap;
    for (const auto &nameToRef : references) {
        const string &fieldName = nameToRef.first;
        const ReferenceBase *ref = nameToRef.second;
        if (ref->isMount()) {
            uint32_t mountId = mountInfoPtr->get(fieldName)->mountId;
            int64_t mountOffset = ref->getMountOffset();
            autil::PackOffset pOffset;
            pOffset.fromUInt64(mountOffset);
            if (pOffset.isImpactFormat()) {
                AUTIL_LOG(WARN, "field [%s] with impact format not support set default value.", fieldName.c_str());
                return false;
            }
            int64_t curLen = pOffset.getOffset() + ref->getAllocateSize();
            auto iter = mountIdToLengthMap.find(mountId);
            if (iter == mountIdToLengthMap.end()) {
                mountIdToLengthMap[mountId] = curLen;
            } else {
                int64_t mountLen = iter->second > curLen ? iter->second : curLen;
                mountIdToLengthMap[mountId] = mountLen;
            }
        }
    }

    for (const auto &mountToLength : mountIdToLengthMap) {
        const uint32_t mountId = mountToLength.first;
        const int64_t mountLength = mountToLength.second;
        char *mountAddr = (char *)(_pool->allocate(mountLength));
        memset(mountAddr, 0, mountLength);
        mountIdToMountBuffer.insert(make_pair(mountId, mountAddr));
    }
    return true;
}

} // namespace matchdoc
