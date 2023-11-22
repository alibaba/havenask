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

#include <string>
#include <unordered_map>

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/LayerTablePlanMetaDef.h"
#include "iquan/jni/DynamicParams.h"

namespace iquan {

class PlanOp : public autil::legacy::Jsonizable {
public:
    PlanOp()
        : id(0)
        , jsonAttrs(nullptr) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == TO_JSON) {
            json.Jsonize("id", id, id);
            json.Jsonize("op_name", opName, std::string());
            json.Jsonize("inputs", inputs, {});
            json.Jsonize("outputs", outputs, {});
            json.Jsonize("IQUAN_ATTRS", jsonAttrs, jsonAttrs);
        } else {
            json.Jsonize("id", id, id);
            json.Jsonize("op_name", opName, std::string());
            json.Jsonize("inputs", inputs, {});
            json.Jsonize("outputs", outputs, {});

            autil::legacy::RapidValue *rapidValue = json.getRapidValue();
            if (unlikely(rapidValue == nullptr || !rapidValue->IsObject()
                         || !rapidValue->HasMember(IQUAN_ATTRS))) {
                throw IquanException("FROM_JSON fail, PlanOp json not contain attrs");
            }
            jsonAttrs = &(*rapidValue)[IQUAN_ATTRS];
            if (unlikely(!jsonAttrs->IsObject())) {
                throw IquanException("FROM_JSON fail, attrs is not object");
            }
        }
    }

    std::string jsonAttr2Str(const DynamicParams *dynamicParams,
                             const DynamicParams *innerDynamicParams) const {
        return jsonAttr2Str(dynamicParams, innerDynamicParams, jsonAttrs);
    }
    std::string jsonAttr2Str(const DynamicParams *dynamicParams,
                             const DynamicParams *innerDynamicParams,
                             autil::legacy::RapidValue *attr) const;

    class PlanOpHandle {
        using Ch = rapidjson::UTF8<>::Ch;
        using SizeType = unsigned;

    public:
        PlanOpHandle(autil::legacy::RapidWriter &writer,
                     const DynamicParams *dynamicParams,
                     const DynamicParams *innerDynamicParams)
            : writer(writer)
            , dynamicParams(dynamicParams)
            , innerDynamicParams(innerDynamicParams) {}

        bool Null() {
            return writer.Null();
        }
        bool Bool(bool b) {
            return writer.Bool(b);
        }
        bool Int(int i) {
            return writer.Int(i);
        }
        bool Uint(unsigned u) {
            return writer.Uint(u);
        }
        bool Int64(int64_t i64) {
            return writer.Int64(i64);
        }
        bool Uint64(uint64_t u64) {
            return writer.Uint64(u64);
        }
        bool Double(double d) {
            return writer.Double(d);
        }
        bool String(const Ch *str, SizeType length, bool copy = false) {
            autil::StringView constString(str, length);
            if (dynamicParams && length >= IQUAN_HINT_PARAMS_MIN_SIZE) {
                if (doReplaceHintParams(constString, copy)) {
                    return true;
                }
            }
            if (innerDynamicParams && length >= IQUAN_REPLACE_PARAMS_MIN_SIZE) {
                if (doReplaceParams(constString, copy)) {
                    return true;
                }
            }
            if (dynamicParams && length >= IQUAN_DYNAMIC_PARAMS_MIN_SIZE) {
                if (doReplaceDynamicParams(constString, copy)) {
                    return true;
                }
            }

            return writer.String(str, length, copy);
        }
        bool Key(const Ch *str, SizeType length, bool copy) {
            return writer.Key(str, length, copy);
        }
        bool StartObject() {
            return writer.StartObject();
        }
        bool EndObject(SizeType memberCount) {
            return writer.EndObject(memberCount);
        }
        bool StartArray() {
            return writer.StartArray();
        }
        bool EndArray(SizeType elementCount = 0) {
            return writer.EndArray(elementCount);
        }

    private:
        bool writeAny(const autil::legacy::Any &any, const std::string &expectedType, bool copy);
        bool doReplaceDynamicParams(autil::StringView &planStr, bool copy);
        bool doReplaceHintParams(autil::StringView &planStr, bool copy);
        bool doReplaceParams(autil::StringView &planStr, bool copy);
        bool parseDynamicParamsContent(autil::StringView &planStr,
                                       const std::string &prefixStr,
                                       const std::string &suffixStr,
                                       std::string &keyStr,
                                       std::string &typeStr);

    private:
        autil::legacy::RapidWriter &writer;
        const DynamicParams *dynamicParams;
        const DynamicParams *innerDynamicParams;
    };

private:
    void patchJson(autil::legacy::RapidWriter &writer, std::set<std::string> &patchKeys) const;

    template <class T>
    void patchJson(autil::legacy::RapidWriter &writer,
                   const std::map<std::string, T> &patchAttrs,
                   std::set<std::string> &patchKeys) const {
        for (const auto &pair : patchAttrs) {
            const auto &key = pair.first;
            auto ret = patchKeys.insert(key);
            if (ret.second) {
                writer.Key(key.c_str(), key.size());
                patchJsonValue(writer, pair.second);
            } else {
                AUTIL_LOG(INFO, "duplicate patch key %s", key.c_str());
            }
        }
    }

    void patchJsonValue(autil::legacy::RapidWriter &writer, const std::string &value) const {
        writer.String(value.c_str(), value.size());
    }

    void patchJsonValue(autil::legacy::RapidWriter &writer, int64_t value) const {
        writer.Int64(value);
    }

    void patchJsonValue(autil::legacy::RapidWriter &writer, double value) const {
        writer.Double(value);
    }

    void patchJsonValue(autil::legacy::RapidWriter &writer, bool value) const {
        writer.Bool(value);
    }

public:
    size_t id;
    std::string opName;
    std::map<std::string, std::vector<size_t>> inputs;
    std::vector<size_t> outputs;
    autil::legacy::RapidValue *jsonAttrs;
    std::map<std::string, std::string> binaryAttrs;

    std::map<std::string, std::string> patchStrAttrs;
    std::map<std::string, double> patchDoubleAttrs;
    std::map<std::string, int64_t> patchInt64Attrs;
    std::map<std::string, bool> patchBoolAttrs;

private:
    AUTIL_LOG_DECLARE();
};

class OptimizeInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("op", op, op);
        json.Jsonize("type", type, type);
        json.Jsonize("params", params, params);
    }

public:
    std::string op;
    std::string type;
    std::vector<std::string> params;
};

class SqlPlanMeta : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("layer_table_plan_meta", layerTableMeta, layerTableMeta);
    }

public:
    std::vector<LayerTablePlanMetaDef> layerTableMeta;
};

class OptimizeInfos : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("optimize_info", optimizeInfo, optimizeInfo);
        json.Jsonize("replace_key", key, key);
        json.Jsonize("replace_type", type, type);
    }

public:
    OptimizeInfo optimizeInfo;
    std::vector<std::string> key;
    std::vector<std::string> type;
};

class SqlPlan : public autil::legacy::Jsonizable {
public:
    SqlPlan() = default;
    SqlPlan(const SqlPlan &otherPlan) = default;
    SqlPlan(SqlPlan &&otherPlan) = default;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("rel_plan_version", relPlanVersion);
        json.Jsonize("rel_plan", opList, opList);
        json.Jsonize("exec_params", execParams, execParams);
        json.Jsonize("optimize_infos", optimizeInfosMap, optimizeInfosMap);
        json.Jsonize("plan_meta", sqlPlanMeta, sqlPlanMeta);
    }

    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(autil::legacy::FastToJsonString(this, true));
    }

    Status deserialize(autil::DataBuffer &dataBuffer) {
        std::string strBuf;
        dataBuffer.read(strBuf);
        try {
            autil::legacy::FastFromJsonString(*this, strBuf);
        } catch (const autil::legacy::ExceptionBase &e) {
            return Status(IQUAN_JSON_FORMAT_ERROR, e.GetMessage());
        } catch (const IquanException &e) { return Status(IQUAN_JSON_FORMAT_ERROR, e.what()); }

        return Status::OK();
    }

public:
    std::string relPlanVersion;
    std::vector<PlanOp> opList;
    std::map<std::string, std::string> execParams;
    std::map<std::string, std::vector<OptimizeInfos>> optimizeInfosMap;
    SqlPlanMeta sqlPlanMeta;
    DynamicParams innerDynamicParams; // for optimizer
    std::shared_ptr<autil::legacy::RapidDocument>
        rawRapidDoc; // need hold, plan node use attribute info
};

typedef std::shared_ptr<SqlPlan> SqlPlanPtr;

class PlanOpWrapper : public autil::legacy::Jsonizable {
public:
    bool convert(const PlanOp &planOp,
                 const DynamicParams &dynamicParams,
                 const DynamicParams &innerDynamicParams) {
        id = planOp.id;
        opName = planOp.opName;
        inputs = planOp.inputs;
        binaryAttrs = planOp.binaryAttrs;

        std::string attrMapStr;
        attrMapStr = planOp.jsonAttr2Str(&dynamicParams, &innerDynamicParams);

        Status status = Utils::fromJson(attrMap, attrMapStr);
        if (!status.ok()) {
            throw IquanException("from json fail: " + attrMapStr
                                 + ", msg: " + status.errorMessage());
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == TO_JSON) {
            json.Jsonize("id", id, id);
            json.Jsonize("op_name", opName, std::string());
            json.Jsonize("inputs", inputs, {});
            json.Jsonize(IQUAN_ATTRS, attrMap, attrMap);
        } else {
            throw IquanException("PlanOpWrapper does not support FROM_JSON");
        }
    }

public:
    size_t id;
    std::string opName;
    std::map<std::string, std::vector<size_t>> inputs;
    autil::legacy::json::JsonMap attrMap;
    std::map<std::string, std::string> binaryAttrs;
};

class SqlPlanWrapper : public autil::legacy::Jsonizable {
public:
    SqlPlanWrapper() = default;

    bool convert(const SqlPlan &sqlPlan, const DynamicParams &dynamicParams) {
        relPlanVersion = sqlPlan.relPlanVersion;
        execParams = sqlPlan.execParams;

        for (const PlanOp &planOp : sqlPlan.opList) {
            PlanOpWrapper planOpWrapper;
            if (!planOpWrapper.convert(planOp, dynamicParams, sqlPlan.innerDynamicParams)) {
                return false;
            }
            opList.emplace_back(planOpWrapper);
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("rel_plan_version", relPlanVersion);
        json.Jsonize("rel_plan", opList, opList);
        json.Jsonize("exec_params", execParams, execParams);
    }

public:
    std::string relPlanVersion;
    std::vector<PlanOpWrapper> opList;
    std::map<std::string, std::string> execParams;
};
} // namespace iquan
