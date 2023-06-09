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
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, QuotaControl);

QuotaControl::QuotaControl(size_t totalQuota) noexcept : _totalQuota(totalQuota), _leftQuota(totalQuota) {}

QuotaControl::~QuotaControl() noexcept {}
}} // namespace indexlib::util
