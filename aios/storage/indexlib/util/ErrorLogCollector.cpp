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
#include "indexlib/util/ErrorLogCollector.h"

#include <iosfwd>

using namespace std;

namespace indexlib { namespace util {
alog::Logger* ErrorLogCollector::_logger = alog::Logger::getLogger("ErrorLogCollector");
string ErrorLogCollector::_identifier = "";
bool ErrorLogCollector::_useErrorLogCollector = false;
bool ErrorLogCollector::_enableTotalErrorCount = false;

atomic_long ErrorLogCollector::_totalErrorCount(0);

}} // namespace indexlib::util
