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
#ifndef NAVI_TESTERDEF_H
#define NAVI_TESTERDEF_H

namespace navi {

static const std::string NAVI_TESTER_BIZ = "navi@tester_biz";
static const std::string NAVI_TESTER_NODE = "navi@tester_node";
static const std::string NAVI_TESTER_RESOURCE = "navi@tester_resource";

static const std::string NAVI_TESTER_INPUT_NODE = "navi@tester_input_node";
static const std::string NAVI_TESTER_INPUT_KERNEL = "navi@tester_input_kernel";
static const std::string NAVI_TESTER_INPUT_KERNEL_OUTPUT = "navi@tester_input_kernel_output";

static const std::string NAVI_TESTER_OUTPUT_NODE = "navi@tester_output_node";
static const std::string NAVI_TESTER_OUTPUT_KERNEL = "navi@tester_output_kernel";
static const std::string NAVI_TESTER_OUTPUT_KERNEL_INPUT = "navi@tester_output_kernel_input";

static const std::string NAVI_TESTER_CONTROL_NODE = "navi@tester_control_node";
static const std::string NAVI_TESTER_CONTROL_KERNEL = "navi@tester_control_kernel";
static const std::string NAVI_TESTER_CONTROL_KERNEL_SIGNAL = "navi@tester_control_kernel_signal";
static const std::string NAVI_TESTER_CONTROL_KERNEL_FAKE_OUTPUT = "navi@tester_control_kernel_fake";

}

#endif //NAVI_TESTERDEF_H
