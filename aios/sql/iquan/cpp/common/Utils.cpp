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
#include "iquan/common/Utils.h"

#include <algorithm>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <typeinfo>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "iquan/common/Common.h"

using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace iquan {

AUTIL_LOG_SETUP(iquan, Utils);

Status Utils::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (unlikely(NULL == ret)) {
        return Status(IQUAN_FAIL, "can not get cwd");
    }
    path = std::string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return Status::OK();
}

Status Utils::getBinaryPath(std::string &path) {
    std::string binaryPath = EnvUtil::getEnv(BINARY_PATH, "");
    if (unlikely(binaryPath.empty())) {
        std::string workDir = "";
        Status status = getCurrentPath(workDir);
        if (!status.ok()) {
            return status;
        }
        binaryPath = workDir + "/../binary";
    }
    path = std::move(binaryPath);
    return Status::OK();
}

Status Utils::getRealPath(const std::string &path, std::string &realPath) {
    char pathBuffer[PATH_MAX];
    if (unlikely(NULL == realpath(path.c_str(), pathBuffer))) {
        std::string msg = "get real path of path [" + path + "] fail.";
        return Status(IQUAN_FAILED_TO_GET_REAL_PATH, msg);
    }
    realPath = std::string(pathBuffer);
    return Status::OK();
}

bool Utils::isExist(const std::string &path) {
    struct stat buf;
    if (stat(path.c_str(), &buf) != 0) {
        return false;
    }
    return true;
}

bool Utils::isFile(const std::string &path) {
    struct stat buf;
    if (stat(path.c_str(), &buf) != 0) {
        return false;
    }
    if (S_ISREG(buf.st_mode)) {
        return true;
    }
    return false;
}

bool Utils::isDir(const std::string &path) {
    struct stat buf;
    if (stat(path.c_str(), &buf) != 0) {
        return false;
    }
    if (S_ISDIR(buf.st_mode)) {
        return true;
    }
    return false;
}

Status Utils::listDir(const std::string &path, std::vector<std::string> &fileList, bool append) {
    DIR *dir = opendir(path.c_str());
    if (dir == nullptr) {
        std::string msg = "open path [" + path + "] fail.";
        return Status(IQUAN_FAIL, msg);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        std::string subName = std::string(entry->d_name);
        std::string fullPath = path + "/" + subName;
        struct stat buf;
        if (stat(fullPath.c_str(), &buf) != 0) {
            continue;
        }

        if (append) {
            fileList.emplace_back(fullPath);
        } else {
            fileList.emplace_back(subName);
        }
    }
    if (closedir(dir) < 0) {
        std::string msg = "close path [" + path + "] fail.";
        return Status(IQUAN_FAIL, msg);
    }
    return Status::OK();
}

Status Utils::readFile(const std::string &path, std::string &fileContent) {
    std::ifstream fileStream(path);
    std::string tmpContent((std::istreambuf_iterator<char>(fileStream)),
                           std::istreambuf_iterator<char>());
    fileContent = std::move(tmpContent);
    fileStream.close();
    if (fileContent.empty()) {
        return Status(-1, "can not read file, " + path);
    }
    return Status::OK();
}

Status Utils::readFiles(const std::vector<std::string> &pathList,
                        std::vector<std::string> &fileContentList,
                        bool reverse,
                        bool ignoreError) {
    fileContentList.clear();
    std::vector<std::string> fileContent;
    for (const std::string &path : pathList) {
        fileContent.clear();
        std::ifstream fileStream(path);
        if (fileStream.is_open()) {
            std::string line;
            while (std::getline(fileStream, line)) {
                fileContent.emplace_back(line);
            }
            if (!reverse) {
                fileContentList.insert(
                    fileContentList.end(), fileContent.begin(), fileContent.end());
            } else {
                fileContentList.insert(
                    fileContentList.end(), fileContent.rbegin(), fileContent.rend());
            }
            fileStream.close();
        } else {
            if (ignoreError) {
                AUTIL_LOG(WARN, "read file [%s] failed, ignored", path.c_str());
                continue;
            } else {
                return Status(-1, "can not read file, " + path);
            }
        }
    }
    return Status::OK();
}

Status Utils::writeFile(const std::string &path, const std::string &fileContent) {
    std::ofstream write;
    write.open(path.c_str(), std::ios::out | std::ios::binary);
    write.write(fileContent.data(), fileContent.size());
    write.close();
    return Status::OK();
}

autil::legacy::json::JsonMap Utils::shallowClone(const autil::legacy::json::JsonMap &mapValue) {
    autil::legacy::json::JsonMap newMapValue;
    for (const auto &kv : mapValue) {
        autil::legacy::Any newAny(kv.second);
        newMapValue[kv.first] = newAny;
    }
    return newMapValue;
}

std::vector<std::vector<autil::legacy::Any>>
Utils::shallowClone(const std::vector<std::vector<autil::legacy::Any>> &listValue) {
    std::vector<std::vector<autil::legacy::Any>> newListValue;
    for (const std::vector<autil::legacy::Any> &innerListValue : listValue) {
        std::vector<autil::legacy::Any> newInnerListValue;
        for (const autil::legacy::Any &any : innerListValue) {
            autil::legacy::Any newAny(any);
            newInnerListValue.emplace_back(newAny);
        }
        newListValue.emplace_back(newInnerListValue);
    }
    return newListValue;
}

autil::legacy::json::JsonMap Utils::defaultSqlParams() {
    autil::legacy::json::JsonMap sqlParams;
    sqlParams["iquan.optimizer.debug.enable"] = IQUAN_FALSE;
    sqlParams["iquan.plan.format.version"] = std::string("plan_version_0.0.1");
    sqlParams[IQUAN_PLAN_FORMAT_TYPE] = IQUAN_JSON;
    sqlParams["iquan.plan.format.object.enable"] = IQUAN_TRUE;
    sqlParams["iquan.plan.output.exec_params"] = IQUAN_FALSE;
    sqlParams[IQUAN_PLAN_PREPARE_LEVEL] = std::string("none");
    sqlParams[IQUAN_PLAN_CACHE_ENALE] = IQUAN_FALSE;
    return sqlParams;
}

/*
Status Utils::fromObjectImpl(const fb::IquanFbAny *iquanAnyPtr,
autil::legacy::Any &object) { if (iquanAnyPtr == NULL) { return
Status(IQUAN_FAIL, "table = null");
    }

    const fb::IquanFbAny *anyObject = iquanAnyPtr;
    fb::IquanFbUAny anyValueType = anyObject->value_type();

    if (anyValueType == fb::IquanFbUAny_IquanFbBool) {
        const fb::IquanFbBool *fbBoolValuePtr =
anyObject->value_as_IquanFbBool(); if (unlikely(fbBoolValuePtr == NULL)) {
            return Status(IQUAN_FAIL, "fbBoolValuePtr = null");
        }
        const bool actualValue = fbBoolValuePtr->value();
        Any actualAnyValue = Any(actualValue);
        object.Swap(actualAnyValue);
    } else if (anyValueType == fb::IquanFbUAny_IquanFbInt) {
        const fb::IquanFbInt *fbIntValuePtr = anyObject->value_as_IquanFbInt();
        if (unlikely(fbIntValuePtr == NULL)) {
            return Status(IQUAN_FAIL, "fbIntValuePtr = null");
        }
        const int actualValue = fbIntValuePtr->value();
        Any actualAnyValue(actualValue);
        object.Swap(actualAnyValue);
    } else if (anyValueType == fb::IquanFbUAny_IquanFbLong) {
        const fb::IquanFbLong *fbLongValuePtr =
anyObject->value_as_IquanFbLong(); if (unlikely(fbLongValuePtr == NULL)) {
            return Status(IQUAN_FAIL, "fbLongValuePtr = null");
        }
        const long actualValue = fbLongValuePtr->value();
        Any actualAnyValue(actualValue);
        object.Swap(actualAnyValue);
    } else if (anyValueType == fb::IquanFbUAny_IquanFbDouble) {
        const fb::IquanFbDouble *fbDoubleValuePtr =
anyObject->value_as_IquanFbDouble(); if (unlikely(fbDoubleValuePtr ==
NULL)) { return Status(IQUAN_FAIL, "fbDoubleValuePtr = null");
        }
        const double actualValue = fbDoubleValuePtr->value();
        Any actualAnyValue(actualValue);
        object.Swap(actualAnyValue);
    } else if (anyValueType == fb::IquanFbUAny_IquanFbString) {
        const fb::IquanFbString *fbStringValuePtr =
anyObject->value_as_IquanFbString(); if (unlikely(fbStringValuePtr ==
NULL)) { return Status(IQUAN_FAIL, "fbStringValuePtr = null");
        }
        const std::string actualValue = fbStringValuePtr->value()->str();
        Any actualAnyValue(actualValue);
        object.Swap(actualAnyValue);
    } else if (anyValueType == fb::IquanFbUAny_IquanFbList) {
        const fb::IquanFbList *fbListValuePtr =
anyObject->value_as_IquanFbList(); AnyList list;
        IQUAN_ENSURE_FUNC(Utils::fromListImpl(fbListValuePtr, list));
        object = std::move(list);
    } else if (anyValueType == fb::IquanFbUAny_IquanFbMap) {
        AnyMap map;
        const fb::IquanFbMap *fbMapValuePtr = anyObject->value_as_IquanFbMap();
        IQUAN_ENSURE_FUNC(Utils::fromMapImpl(fbMapValuePtr, map));
        object = std::move(map);
    } else {
        throw IquanException("unsupported type in fromObjectImpl: " +
std::to_string(anyValueType));
    }

    return Status::OK();
}

Status Utils::fromMapImpl(const fb::IquanFbMap *fbMap, AnyMap &map) {
    if (unlikely(fbMap == NULL)) {
        return Status(IQUAN_FAIL, "fbMap = null");
    }

    // 1. get array of IquanFbMapEntry
    auto *fbMapEntryArray = fbMap->value();
    if (unlikely(fbMapEntryArray == NULL)) {
        return Status(IQUAN_FAIL, "fbMapEntryArray = null");
    }

    // 2. try to parse IquanFbMapEntry
    for (uint64_t i = 0; i < fbMapEntryArray->size(); i++) {
        // 2.1 get entry of map
        auto  *entry = fbMapEntryArray->Get(i);
        if (unlikely(entry == NULL)) {
            return Status(IQUAN_FAIL, "entry = null");
        }

        // 2.2 parse entry of map
        const std::string &key = entry->key()->str();
        const fb::IquanFbAny *fbValue = entry->value();
        if (unlikely(fbValue == NULL)) {
            return Status(IQUAN_FAIL, "fbValue = null");
        }

        // 2.3 get actual value of fbValue
        Any singleAny;
        IQUAN_ENSURE_FUNC(Utils::fromObjectImpl(fbValue, singleAny));

        // 2.4 save it
        map[key] = singleAny;
    }

    return Status::OK();
}

Status Utils::fromListImpl(const fb::IquanFbList *fbList, AnyList &list) {
    if (unlikely(fbList == NULL)) {
        return Status(IQUAN_FAIL, "fbList = null");
    }

    auto *iquanFbArray = fbList->value();
    if (unlikely(iquanFbArray == NULL)) {
        return Status(IQUAN_FAIL, "iquanFbArray = null");
    }

    for (uint64_t i = 0; i < iquanFbArray->size(); i++) {
        Any singleAny;
        IQUAN_ENSURE_FUNC(Utils::fromObjectImpl(iquanFbArray->Get(i),
singleAny)); list.emplace_back(singleAny);
    }
    return Status::OK();
}

Status Utils::fromInputsImpl(const fb::IquanFbMap *fbInputs,
                             std::map<std::string, std::vector<size_t>> &inputs)
{
    // 1. parse the input map
    AnyMap inputMap;
    IQUAN_ENSURE_FUNC(Utils::fromMapImpl(fbInputs, inputMap));

    // 2. parse map
    AnyMap::iterator mapIterator = inputMap.begin();
    for (; mapIterator != inputMap.end(); mapIterator++) {
        const std::string &key = mapIterator->first;
        Any &value = mapIterator->second;

        if (unlikely(typeid(AnyList) != value.GetType())) {
            return Status(IQUAN_FAIL, "input is not array");
        }

        AnyList arrays = autil::legacy::AnyCast<AnyList>(value);
        std::vector<size_t> inputsArray;
        for (uint32_t i = 0; i < arrays.size(); i++) {
            Any inputNo = arrays[i];
            if (likely(inputNo.GetType() == typeid(int32_t))) {
                inputsArray.emplace_back(
                        autil::legacy::AnyCast<int>(inputNo));
            } else {
                return Status(IQUAN_FAIL, "input no is not number");
            }
            inputs[key] = inputsArray;
        }
    }

    return Status::OK();
}

Status Utils::fromPlanOpImpl(const fb::IquanFbPlanOp *fbPlanOp, PlanOp &op) {
    if (unlikely(fbPlanOp == NULL)) {
        return Status(IQUAN_FAIL, "fbSqlPlan = null");
    }

    // 1. id
    op.id = fbPlanOp->id();

    // 2. opName
    op.opName = fbPlanOp->op_name()->str();

    // 3. inputs
    IQUAN_ENSURE_FUNC(Utils::fromInputsImpl(fbPlanOp->inputs(), op.inputs));

    // 4. jsonAttrs
    IQUAN_ENSURE_FUNC(Utils::fromMapImpl(fbPlanOp->json_attrs(), op.jsonAttrs));

    return Status::OK();
}

Status Utils::fromExecParamsImpl(const fb::IquanFbMap *fbSqlPlan, SqlPlan
&sqlPlan) {
    // todo:
    return Status::OK();
}

Status Utils::fromSqlPlanImpl(const fb::IquanFbSqlPlan *fbSqlPlan, SqlPlan
&sqlPlan) { if (unlikely(fbSqlPlan == NULL)) { return Status(IQUAN_FAIL,
"fbSqlPlan = null");
    }

    // 1. copy the relPlanVersion
    sqlPlan.relPlanVersion = fbSqlPlan->rel_plan_version()->str();

    // 2. copy planop array
    auto ops = fbSqlPlan->op_list();
    if (unlikely(ops == NULL)) {
        return Status(IQUAN_FAIL, "ops = null");
    }

    sqlPlan.opList.clear();
    for (uint64_t i = 0; i < ops->size(); i++) {
        // 2.1 try to get one of ops struct
        auto iquanFbPlanOp = ops->Get(i);

        // 2.2 check status code
        PlanOp op;
        IQUAN_ENSURE_FUNC(Utils::fromPlanOpImpl(iquanFbPlanOp, op));

        // 2.3 save it
        sqlPlan.opList.emplace_back(op);
    }

    // 3. copy the exec params (todo)

    return Status::OK();
}

Status Utils::fromIquanDqlResponseImpl(const fb::IquanFbIquanDqlResponse
*fbResponse, IquanDqlResponse &response) { do {
        // 1. copy error code
        response.errorCode = fbResponse->error_code();
        response.errorMsg = fbResponse->error_msg()->str();

        // 2. return right away if we detect an error in fb struct
        if (response.errorCode != IQUAN_OK) {
            break;
        }

        // 3. now, we will try to parse plan itself
        IQUAN_ENSURE_FUNC(
                Utils::fromSqlPlanImpl(fbResponse->result(), response.result));
    } while(0);

    return Status::OK();
}


Status Utils::fromFbIquanDqlResponse(IquanDqlResponse &response, const
std::string &fbByteArray) { try { const void *dataPtr = (const void
*)(fbByteArray.data()); if (unlikely(dataPtr == NULL)) { return
Status(IQUAN_FAIL, "dataPtr is null");
        }

        // 1. try to convert to IquanFbIquanDqlResponse
        const fb::IquanFbIquanDqlResponse *fbResponsePtr =
                fb::GetIquanFbIquanDqlResponse(dataPtr);
        if (unlikely(fbResponsePtr == NULL)) {
            return Status(IQUAN_FAIL, "fbResponsePtr is null");
        }

        // 2. convert to IquanDqlResponse from IquanFbIquanDqlResponse and
        // return it
        IQUAN_ENSURE_FUNC(
                Utils::fromIquanDqlResponseImpl(fbResponsePtr, response));
    } catch (const IquanException &e) {
        return Status(IQUAN_FLATTENBUFFER_FORMAT_ERROR, e.what());
    }
    return Status::OK();
}
*/

std::string Utils::anyToString(const autil::legacy::Any &any, const std::string &expectedType) {
    if (IsJsonBool(any) && expectedType == "boolean") {
        bool value = autil::legacy::AnyCast<bool>(any);
        return value ? "true" : "false";
    } else if (IsJsonString(any) && isStringType(expectedType)) {
        std::string value = autil::legacy::AnyCast<std::string>(any);
        return "\"" + value + "\"";
    } else if (IsJsonNumber(any) && isNumberType(expectedType)) {
        JsonNumber jsonNumber = autil::legacy::AnyCast<JsonNumber>(any);
        return jsonNumber.AsString();
    }

    if (!isNumberType(expectedType)) {
        const std::string &typeName = GetJsonTypeName(any);
        throw IquanException(std::string("any type(" + typeName + ") cast to expect type("
                                         + expectedType + ") is not support"));
    }

#ifdef RETURN_TYPE_STRING
#undef RETURN_TYPE_STRING
#endif
#define RETURN_TYPE_STRING(TARGETTYPE)                                                             \
    if (type == typeid(TARGETTYPE)) {                                                              \
        TARGETTYPE value = autil::legacy::AnyCast<TARGETTYPE>(any);                                \
        return StringUtil::toString(value);                                                        \
    }

    const std::type_info &type = any.GetType();
    RETURN_TYPE_STRING(int8_t);
    RETURN_TYPE_STRING(uint8_t);
    RETURN_TYPE_STRING(int16_t);
    RETURN_TYPE_STRING(uint16_t);
    RETURN_TYPE_STRING(int32_t);
    RETURN_TYPE_STRING(uint32_t);
    RETURN_TYPE_STRING(int64_t);
    RETURN_TYPE_STRING(uint64_t);
    RETURN_TYPE_STRING(float);
    RETURN_TYPE_STRING(double);

#undef RETURN_TYPE_STRING

    const std::string &typeName = GetJsonTypeName(any);
    throw IquanException(std::string("any type(" + typeName + ") cast to expect type("
                                     + expectedType + ") is not support"));
}

/*
Status Utils::deepCopyAny(const autil::legacy::Any &src, autil::legacy::Any
&dst) { if (autil::legacy::json::IsJsonString(src)) { const std::string oldStr =
            autil::legacy::AnyCast<std::string>(src);
        dst = oldStr;
    } else if (autil::legacy::json::IsJsonArray(src)) {
        autil::legacy::json::JsonArray newJsonArray;
        const autil::legacy::json::JsonArray &oldJsonArray =
                autil::legacy::AnyCast<autil::legacy::json::JsonArray>(src);
        IQUAN_ENSURE_FUNC(deepCopyAnyArray(oldJsonArray, newJsonArray));
        dst = newJsonArray;
    } else if (autil::legacy::json::IsJsonMap(src)) {
        autil::legacy::json::JsonMap newJsonMap;
        const autil::legacy::json::JsonMap &oldJsonMap =
                autil::legacy::AnyCast<autil::legacy::json::JsonMap>(src);
        IQUAN_ENSURE_FUNC(deepCopyAnyMap(oldJsonMap, newJsonMap));
        dst = newJsonMap;
    } else if (autil::legacy::json::IsJsonNumber(src)) {
        const autil::legacy::json::JsonNumber &oldNumber =
                autil::legacy::AnyCast<autil::legacy::json::JsonNumber>(src);
        dst = oldNumber;
    } else if (autil::legacy::json::IsJsonBool(src)) {
        const bool oldBool = autil::legacy::AnyCast<bool>(src);
        dst = oldBool;
    } else {
        return Status(IQUAN_FAIL, "can not support this type:" +
autil::legacy::json::GetJsonTypeName(src));
    }
    return Status::OK();
}

Status Utils::deepCopyAnyMap(const autil::legacy::json::JsonMap &src,
                             autil::legacy::json::JsonMap &dst)
{
    autil::legacy::json::JsonMap::const_iterator iter = src.begin();
    for (; iter != src.end(); iter++) {
        const autil::legacy::Any &oldAny = iter->second;
        autil::legacy::Any newAny;
        IQUAN_ENSURE_FUNC(Utils::deepCopyAny(oldAny, newAny));
        dst[iter->first] = newAny;
     }
     return Status::OK();
 }

 Status Utils::deepCopyAnyArray(const autil::legacy::json::JsonArray &src,
                         autil::legacy::json::JsonArray &dst)
 {
     autil::legacy::json::JsonArray::const_iterator iter = src.begin();
     for (; iter != src.end(); iter++) {
         const autil::legacy::Any &oldAny = *iter;
         autil::legacy::Any newAny;
         IQUAN_ENSURE_FUNC(Utils::deepCopyAny(oldAny, newAny));
         dst.emplace_back(newAny);
     }
     return Status::OK();
}
*/

} // namespace iquan
