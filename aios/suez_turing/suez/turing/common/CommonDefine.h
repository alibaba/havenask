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
#include <stdint.h>
#include <string>

constexpr uint32_t kDefaultReturnCount = 500;
constexpr uint32_t SLEEP_INTERVAL = 1000 * 1000; // 1 sec

extern const std::string DEFAULT_SRC;
extern const std::string kReturnFieldsSep;
extern const std::string kAliasSep;
extern const std::string kDefaultValuesNode;
extern const std::string kOrderByNode;
extern const std::string kDistinctNode;
extern const std::string kReturnFieldsNode;
extern const std::string kFilterNode;
extern const std::string kMaxReturnCountNode;
extern const std::string kTableNameNode;
extern const std::string kPkNode;
extern const std::string kPkeySplitNode;
extern const std::string kLeftQueryNode;
extern const std::string kRightQueryNode;
extern const std::string kJoinKeyNode;
extern const std::string kDefaultMultiConfig;
extern const std::string kSuezTuringDefaultServerName;

// for local debug
constexpr char kEnableLocalDebug[] = "ENABLE_LOCAL_DEBUG";
constexpr char kAgentAddress[] = "AGENT_ADDRESS";
constexpr char kRemoteTables[] = "REMOTE_TABLES";
constexpr char kRemoteIGraph[] = "REMOTE_IGRAPH_ADDRESS";

namespace suez {
namespace turing {

typedef std::map<std::string, std::string> KeyValueMap;

extern const std::string ITEM_ID_GROUP_NAME;
extern const std::string ITEM_ID_FIELD_NAME;
extern const std::string MATCHDOC_POS_FIELD_NAME;

// multi graph key definitions, for rtp
extern const std::string SEARCH_GRAPH;
extern const std::string DEBUG_GRAPH;
extern const std::string PROXY_GRAPH;

enum RunMode {
    NORMAL_MODE = 1,
    TIMELINE_MODE = 2,
    PV_RECORD_MODE = 3,
    WARMUP_MODE = 4,
};

inline std::string toString(RunMode mode) {
#define MODE_TO_STRING(mode)                                                                                           \
    case mode##_MODE:                                                                                                  \
        return #mode
    switch (mode) {
        MODE_TO_STRING(NORMAL);
        MODE_TO_STRING(TIMELINE);
        MODE_TO_STRING(PV_RECORD);
        MODE_TO_STRING(WARMUP);
    default:
        return "UNKNOWN";
    }
#undef MODE_TO_STRING
}

extern const std::string RUNMETA_PREFIX;

#define EAGLEEYE_USERDATA_PAIRS_SEPARATOR ((char)0x0012)
#define EAGLEEYE_USERDATA_KV_SEPARATOR ((char)0x0014)

#define TB_EAGLEEYEX_T "t"
#define TB_EAGLEEYEX_U_BZ "u_bz"

enum MatchdocsVersion {
    MATCHDOCS_VERSION_DEFAULT = 0,
    MATCHDOCS_VERSION_FALSE = 1,
    MATCHDOCS_VERSION_TRUE = 2
};

} // namespace turing
} // namespace suez
