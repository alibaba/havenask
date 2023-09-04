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
#include "suez/admin/SignatureGenerator.h"

#include <algorithm>
#include <map>
#include <sstream>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/md5.h"
#include "suez/sdk/JsonNodeRef.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, BizSignature);

static const std::string OPTIONAL_PREFIX = "*";
static const std::string OLD_OPTIONAL_PREFIX = "#";

std::vector<std::string> SignatureGenerator::defaultSignatureRule(bool tableIncNeedRolling,
                                                                  bool bizConfigNeedRolling,
                                                                  bool indexRootNeedRolling,
                                                                  bool serviceInfoRolling) {
    vector<string> nodePaths;
    if (tableIncNeedRolling) {
        nodePaths.push_back("table_info.$$table_name.$$generation_id.partitions.$$partition_id.inc_version");
    } else {
        nodePaths.push_back("table_info.$$table_name.$$generation_id.partitions.$$partition_id");
    }
    if (bizConfigNeedRolling) {
        nodePaths.push_back("biz_info.$$biz_name.config_path");
    } else {
        nodePaths.push_back("biz_info.$$biz_name");
    }
    nodePaths.push_back("table_info.$$table_name.$$generation_id.config_path");
    nodePaths.push_back("table_info.$$table_name.$$generation_id.partitions.$$partition_id.*branch_id");
    if (indexRootNeedRolling) {
        nodePaths.push_back("table_info.$$table_name.$$generation_id.index_root");
    }
    if (serviceInfoRolling) {
        nodePaths.push_back("*service_info.*sub_cm2_configs");
    }
    return nodePaths;
}

bool SignatureGenerator::generateSignature(const vector<string> &nodePaths,
                                           const JsonNodeRef::JsonMap *root,
                                           string &signatureStr) {

    vector<string> results;
    for (const auto &nodePath : nodePaths) {
        if (!generateSignature(nodePath, root, results)) {
            return false;
        }
    }
    auto md5Signature = autil::EnvUtil::getEnv("md5Signature", false);
    if (md5Signature) {
        string tmp = StringUtil::toString(results, " ");
        Md5Stream stream;
        stream.Put((const uint8_t *)tmp.c_str(), tmp.length());
        signatureStr = stream.GetMd5String();
    } else {
        signatureStr = StringUtil::toString(results, " ");
    }
    return true;
}

bool SignatureGenerator::generateSignature(const string &nodePath,
                                           const JsonNodeRef::JsonMap *root,
                                           vector<string> &results) {
    vector<string> nodePathVec = StringTokenizer::tokenize(StringView(nodePath), '.');
    if (nodePathVec.empty()) {
        AUTIL_LOG(WARN, "empty node path");
        return false;
    }
    vector<string> keyStack;
    return generateSignature(nodePathVec, 0, root, keyStack, results);
}

bool SignatureGenerator::generateSignature(const vector<string> &nodePath,
                                           size_t level,
                                           const JsonNodeRef::JsonMap *root,
                                           vector<string> &keyStack,
                                           vector<string> &results) {
    string currentNode = nodePath[level];
    if (StringUtil::startsWith(currentNode, "$$")) {
        for (const auto &item : *root) {
            auto child = autil::legacy::AnyCast<JsonNodeRef::JsonMap>(&item.second);
            if (!child) {
                AUTIL_LOG(ERROR, "%s is not a map", FastToJsonString(item.second, true).c_str());
                return false;
            }
            keyStack.push_back(item.first);
            if (level + 1 == nodePath.size()) {
                results.push_back(StringUtil::toString(keyStack, "."));
            } else if (!generateSignature(nodePath, level + 1, child, keyStack, results)) {
                return false;
            }
            keyStack.pop_back();
        }
    } else {
        bool optional = StringUtil::startsWith(currentNode, OPTIONAL_PREFIX);
        if (optional) {
            currentNode = currentNode.substr(OPTIONAL_PREFIX.size());
        } else {
            /*
             *兼容老的错误逻辑
             *#前缀表示可选节点，当节点找不到时就直接跳过，但是老的实现方式在找节点时没有去掉#前缀，导致一定找不到。
             *修复后改成*前缀来表示可选节点，但是还要保留#前缀的原有行为来保持生产环境行为不变
             * */
            optional = StringUtil::startsWith(currentNode, OLD_OPTIONAL_PREFIX);
        }

        auto it = root->find(currentNode);
        if (root->end() == it) {
            if (!optional) {
                keyStack.push_back(ToJsonString(*root));
                results.push_back(StringUtil::toString(keyStack, "."));
                keyStack.pop_back();
            }
            return true;
        }
        if (level + 1 == nodePath.size()) {
            string value = ToJsonString(it->second);
            // for predeploy, complex condition in json node is not necessary for now
            if ("inc_version" == currentNode && "-1" == value) {
                return true;
            }
            keyStack.push_back(value);
            results.push_back(StringUtil::toString(keyStack, "."));
            keyStack.pop_back();
        } else {
            auto child = autil::legacy::AnyCast<JsonNodeRef::JsonMap>(&it->second);
            if (!child) {
                AUTIL_LOG(ERROR, "%s is not a map", FastToJsonString(it->second, true).c_str());
                return false;
            }
            if (!generateSignature(nodePath, level + 1, child, keyStack, results)) {
                return false;
            }
        }
    }
    return true;
}

} // namespace suez
