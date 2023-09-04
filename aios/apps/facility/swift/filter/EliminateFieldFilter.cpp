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
#include "swift/filter/EliminateFieldFilter.h"

#include <cstddef>
#include <utility>
#include <vector>

using namespace std;
using namespace swift::common;
namespace swift {
namespace filter {
AUTIL_LOG_SETUP(swift, EliminateFieldFilter);

EliminateFieldFilter::EliminateFieldFilter() { _maxFieldOffset = -1; }

EliminateFieldFilter::~EliminateFieldFilter() {}

void EliminateFieldFilter::addRequiredField(const autil::StringView &fieldName, size_t offset) {
    _requiredFieldMap[fieldName] = offset;
    if (_maxFieldOffset < (int32_t)offset) {
        _maxFieldOffset = offset;
    }
}

bool EliminateFieldFilter::getFilteredFields(const string &productionData, string &consumptionData) {
    consumptionData.clear();
    if (!_fieldGroupReader.fromProductionString(productionData)) {
        return false;
    }
    _fieldGroupWriter.reset();
    bool res = filterFields(_fieldGroupReader, _fieldGroupWriter);
    if (res) {
        _fieldGroupWriter.toString(consumptionData);
    }
    return true;
}

bool EliminateFieldFilter::filterFields(const FieldGroupReader &fieldGroupReader, FieldGroupWriter &fieldGroupWriter) {
    bool hasUpdated = false;
    const Field *field = NULL;
    vector<const Field *> fieldVec;
    if (_maxFieldOffset < 0) {
        return false;
    }
    fieldVec.resize(_maxFieldOffset + 1, NULL);
    size_t size = fieldGroupReader.getFieldSize();
    RequiredFieldMap::const_iterator it = _requiredFieldMap.end();
    for (size_t i = 0; i < size; ++i) {
        field = fieldGroupReader.getField(i);
        it = _requiredFieldMap.find(field->name);
        if (_requiredFieldMap.end() != it) {
            fieldVec[it->second] = field;
            if (field->isUpdated) {
                hasUpdated = true;
            }
        }
    }
    if (!hasUpdated) {
        return false;
    }
    fieldGroupWriter.reset();
    for (vector<const Field *>::const_iterator it = fieldVec.begin(); it != fieldVec.end(); ++it) {
        if (NULL != (*it)) {
            fieldGroupWriter.addConsumptionField(&(*it)->value, (*it)->isUpdated);
        } else {
            fieldGroupWriter.addConsumptionField(NULL, false);
        }
    }
    return true;
}

} // namespace filter
} // namespace swift
