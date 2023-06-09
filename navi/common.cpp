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
#include "navi/common.h"

namespace navi {

std::ostream &operator<<(std::ostream &os, const PortIndex &portIndex) {
    return os << (int)portIndex.isGroup() << "," << (int)portIndex.isControl() << "," << portIndex.group << ","
              << portIndex.index;
}

const std::string NAVI_NAMESPACE_SEPERATOR = ".";
const std::string NAVI_DEFAULT_BIZ = "navi@default_biz";
const std::string NODE_PORT_SEPERATOR = ".";
const std::string GROUP_PORT_SEPERATOR = ":";
const std::string NODE_FINISH_PORT = "node@finish";
const std::string TERMINATOR_NODE_NAME = "navi@terminator";
const std::string TERMINATOR_KERNEL_NAME = "navi@TerminatorKernel";
const std::string TERMINATOR_INPUT_PORT = "navi@terminator@input";
const std::string TERMINATOR_OUTPUT_PORT = "navi@terminator@output";
const std::string PLACEHOLDER_KERNEL_NAME = "navi@placeholder";
const std::string PLACEHOLDER_OUTPUT_PORT = "navi@placeholder_out";
const std::string NAVI_USER_GRAPH_BIZ = "navi@user_biz";
const std::string NAVI_FORK_BIZ = "navi@fork";
const std::string NAVI_FORK_NODE = "navi@fork_node";
const std::string NAVI_SERVER_MIRROR_BIZ_PREFIX = "mirror_biz@";

const std::string MAP_KERNEL_INPUT_PORT = "in";
const std::string MAP_KERNEL_OUTPUT_PORT = "out";

const std::string NODE_CONTROL_INPUT_PORT = "node@control_input";
const std::string CONTROL_DATA_TYPE = "node@control_data_type";
const std::string CONTROL_MERGE_KERNEL = "navi@control_merge";

const std::string NODE_RESOURCE_INPUT_PORT = "node@resource_input";

const std::string DEFAULT_SPLIT_KERNEL = "navi@split";
const std::string DEFAULT_MERGE_KERNEL = "navi@merge";
const std::string PORT_KERNEL_INPUT_PORT = "in";
const std::string PORT_KERNEL_OUTPUT_PORT = "out";
const std::string PORT_KERNEL_PART_COUNT_KEY = "part_count";

const std::string DEFAULT_LOG_PATTERN = "[%%c] [%%d] [%%l] [%%t,%%F -- %%f(%%o):%%n] [%%z] [%%m] [%%b]";
const std::string DEFAULT_LOG_FILE_NAME = "logs/navi.log";

const std::string NAVI_RESULT_TYPE_ID = "navi.buildin.type.navi_result";
const std::string ROOT_KMON_RESOURCE_ID = "navi.buildin.resource.root_kmon";
const std::string BIZ_KMON_RESOURCE_ID = "navi.buildin.resource.biz_kmon";
const std::string MEMORY_POOL_RESOURCE_ID = "navi.buildin.resource.mem_pool";
const std::string GRAPH_MEMORY_POOL_RESOURCE_ID = "navi.buildin.resource.graph_mem_pool";
const std::string GIG_CLIENT_RESOURCE_ID = "navi.buildin.gig_client";
const std::string GIG_SESSION_RESOURCE_ID = "navi.buildin.gig_session";
const std::string OBJECT_POOL_RESOURCE_ID = "navi.buildin.resource.object_pool";
const std::string META_INFO_RESOURCE_ID = "navi.buildin.resource.meta_info";

const std::string NAVI_RPC_METHOD_NAME = "navi";

const std::string SIMD_PADING_STR(128, '\0');
}
