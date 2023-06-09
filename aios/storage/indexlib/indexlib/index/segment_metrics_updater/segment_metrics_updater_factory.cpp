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
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater_factory.h"

using namespace std;
using namespace indexlib::plugin;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SegmentMetricsUpdaterFactory);

const string SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX = "Factory_MetricsUpdater";

SegmentMetricsUpdaterFactory::SegmentMetricsUpdaterFactory() {}

SegmentMetricsUpdaterFactory::~SegmentMetricsUpdaterFactory() {}
}} // namespace indexlib::index
