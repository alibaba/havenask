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
#include "FieldMetaReaderWrapper.h"
#include "indexlib/index/field_meta/meta/FieldTokenCountMeta.h"


namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, FieldMetaReaderWrapper);

FieldMetaReaderWrapper::FieldMetaReaderWrapper(autil::mem_pool::Pool *pool , 
                                                FieldMetaReadersMapPtr fieldMetaReadersMapPtr)
                                               :_pool(pool), 
                                               _fieldMetaReadersMapPtr(fieldMetaReadersMapPtr)  {
}

bool FieldMetaReaderWrapper::getFieldLengthOfDoc(int64_t docId, int64_t& totalFieldLength, std::string fieldName) {
    totalFieldLength = 0;
    auto iter = _fieldMetaReadersMapPtr->find(fieldName);
    if(iter == _fieldMetaReadersMapPtr->end()) {
        AUTIL_LOG(ERROR, "failed to get meta reader of field %s, maybe field meta not in schema", fieldName.c_str());
        return false;
    }
    auto fieldMetaReader = iter->second;
    if(!fieldMetaReader) {
        AUTIL_LOG(ERROR, "failed to get meta reader of field %s, field meta reader is null", fieldName.c_str());
        return false;
    }
    uint64_t fieldLength = 0;
    if(!fieldMetaReader->GetFieldTokenCount(docId, _pool, fieldLength)) {
        AUTIL_LOG(ERROR, "failed to get field length of docId: %ld", docId)
        return false;
    }
    totalFieldLength += fieldLength;
    return true;
}

bool FieldMetaReaderWrapper::getFieldAvgLength(double& avgFieldLength, std::string fieldName) {
    auto iter = _fieldMetaReadersMapPtr->find(fieldName);
    if(iter == _fieldMetaReadersMapPtr->end()) {
        AUTIL_LOG(ERROR, "failed to get meta reader of field %s, maybe field meta not in schema", fieldName.c_str());
        return false;
    }
    auto fieldMetaReader = iter->second;
    if(!fieldMetaReader) {
        AUTIL_LOG(ERROR, "failed to get meta reader of field meta %s, field meta reader is null", fieldName.c_str());
        return false;
    }
    auto fieldMeta = fieldMetaReader->GetTableFieldMeta(indexlib::index::FIELD_TOKEN_COUNT_META_STR);
    if(!fieldMeta) {
        AUTIL_LOG(ERROR, "failed to get average field length meta of field %s", fieldName.c_str());
        return false;
    }
    auto fieldLengthMeta = std::dynamic_pointer_cast<indexlib::index::FieldTokenCountMeta>(fieldMeta);
    if(!fieldLengthMeta) {
        AUTIL_LOG(ERROR, "failed to cast average field length meta of field %s", fieldName.c_str());
        return false;
    }
    avgFieldLength = fieldLengthMeta->GetAvgFieldTokenCount();
    return true;
}

} // namespace turing
} // namespace suez
