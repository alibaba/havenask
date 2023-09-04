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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
//#include "iquan/common/fb/iquan_generated.h"

namespace iquan {

class Utils {
public:
    static Status getCurrentPath(std::string &path);
    static Status getBinaryPath(std::string &path);
    static Status getRealPath(const std::string &path, std::string &realPath);

    static bool isExist(const std::string &path);
    static bool isFile(const std::string &path);
    static bool isDir(const std::string &path);
    static Status
    listDir(const std::string &path, std::vector<std::string> &fileList, bool append = false);
    static Status readFile(const std::string &path, std::string &fileContent);
    static Status readFiles(const std::vector<std::string> &path,
                            std::vector<std::string> &fileContentList,
                            bool reverse = false,
                            bool ignoreError = false);
    static Status writeFile(const std::string &path, const std::string &fileContent);

    static autil::legacy::json::JsonMap defaultSqlParams();

    static autil::legacy::json::JsonMap shallowClone(const autil::legacy::json::JsonMap &mapValue);
    static std::vector<std::vector<autil::legacy::Any>>
    shallowClone(const std::vector<std::vector<autil::legacy::Any>> &listValue);

    static std::string anyToString(const autil::legacy::Any &any, const std::string &typeStr);

    template <typename T>
    static Status
    readValue(const autil::legacy::json::JsonMap &map, const std::string &key, T &value) {
        autil::legacy::json::JsonMap::const_iterator iter = map.find(key);
        if (iter == map.end()) {
            return Status(IQUAN_CACHE_KEY_NOT_FOUND, "key: " + key);
        }
        const autil::legacy::Any &any = iter->second;
        try {
            value = autil::legacy::AnyCast<T>(any);
            return Status::OK();
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_BAD_ANY_CAST, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_BAD_ANY_CAST, e.what()); }
        return Status::OK();
    }

    template <typename T>
    static Status toJson(const T &jsonObject, std::string &jsonStr, bool isCompact = true) {
        try {
            jsonStr = autil::legacy::FastToJsonString(jsonObject, isCompact);
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_JSON_FORMAT_ERROR, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_JSON_FORMAT_ERROR, e.what()); }
        return Status::OK();
    }

    template <typename T>
    static Status fromJson(T &jsonObject, const std::string &jsonStr) {
        try {
            autil::legacy::FastFromJsonString(jsonObject, jsonStr);
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_JSON_FORMAT_ERROR, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_JSON_FORMAT_ERROR, e.what()); }
        return Status::OK();
    }

    template <typename T>
    static Status fastToJson(const T &jsonObject, std::string &jsonStr, bool isCompact = true) {
        try {
            jsonStr = autil::legacy::FastToJsonString(jsonObject, isCompact);
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_JSON_FORMAT_ERROR, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_JSON_FORMAT_ERROR, e.what()); }
        return Status::OK();
    }

    template <typename T>
    static Status fastFromJson(T &jsonObject, const std::string &jsonStr) {
        try {
            autil::legacy::FastFromJsonString(jsonObject, jsonStr);
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_JSON_FORMAT_ERROR, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_JSON_FORMAT_ERROR, e.what()); }
        return Status::OK();
    }

    template <typename T>
    static Status fromRapidValue(T &jsonObject,
                                 const std::shared_ptr<autil::legacy::RapidDocument> &docPtr) {
        try {
            FromRapidValue(jsonObject, *docPtr);
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_JSON_FORMAT_ERROR, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_JSON_FORMAT_ERROR, e.what()); }
        return Status::OK();
    }

    // deep copy any
    // static Status deepCopyAnyMap(const autil::legacy::json::JsonMap &src,
    //                       autil::legacy::json::JsonMap &dst);
    // static Status deepCopyAnyArray(const autil::legacy::json::JsonArray &src,
    //                         autil::legacy::json::JsonArray &dst);
    // static Status deepCopyAny(const autil::legacy::Any &src,
    // autil::legacy::Any &dst); static bool doReplaceDynamicParams(std::string
    // &planStr);

    // flattenbuffer interface
    // static Status fromFbIquanDqlResponse(IquanDqlResponse &response, const
    // std::string &fbByteArray); static Status fromObjectImpl(const
    // fb::IquanFbAny *iquanAnyPtr, autil::legacy::Any &object); static Status
    // fromMapImpl(const fb::IquanFbMap *fbMap, AnyMap &map); static Status
    // fromListImpl(const fb::IquanFbList *fbList, AnyList &list); static Status
    // fromInputsImpl(const fb::IquanFbMap *input, std::map<std::string,
    // std::vector<size_t>> &inputs); static Status fromPlanOpImpl(const
    // fb::IquanFbPlanOp *fbPlanOp, PlanOp &op); static Status
    // fromExecParamsImpl(const fb::IquanFbMap *fbSqlPlan, SqlPlan &sqlPlan);
    // static Status fromSqlPlanImpl(const fb::IquanFbSqlPlan *fbSqlPlan,
    // SqlPlan &sqlPlan); static Status fromIquanDqlResponseImpl(const
    // fb::IquanFbIquanDqlResponse *fbResponse, IquanDqlResponse &response);

public:
    // Refer to FieldType in Java
    static inline bool isStringType(const std::string &expectedType) {
        if (expectedType == "string" || expectedType == "text" || expectedType == "char"
            || expectedType == "varchar") {
            return true;
        }
        return false;
    }

    // Refer to FieldType in Java
    static inline bool isNumberType(const std::string &expectedType) {
        if (expectedType == "int8" || expectedType == "int16" || expectedType == "int32"
            || expectedType == "int64" || expectedType == "float" || expectedType == "double"
            || expectedType == "uint8" || expectedType == "uint16" || expectedType == "unit32"
            || expectedType == "uint64") {
            return true;
        }
        return false;
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
