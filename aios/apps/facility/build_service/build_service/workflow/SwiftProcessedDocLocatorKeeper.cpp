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
#include "build_service/workflow/SwiftProcessedDocLocatorKeeper.h"

#include "build_service/util/LocatorUtil.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, SwiftProcessedDocLocatorKeeper);

SwiftProcessedDocLocatorKeeper::SwiftProcessedDocLocatorKeeper() {}

SwiftProcessedDocLocatorKeeper::~SwiftProcessedDocLocatorKeeper() {}

void SwiftProcessedDocLocatorKeeper::init(uint64_t sourceSignature, const common::SwiftParam& param,
                                          int64_t startTimestamp)
{
    _sourceSignature = sourceSignature;
    _swiftParam = param;
    _startTimestamp = startTimestamp;
}

bool SwiftProcessedDocLocatorKeeper::setSeekLocator(const common::Locator& seekLocator)
{
    common::Locator sourceLocator(_sourceSignature, 0);
    if (!seekLocator.IsSameSrc(sourceLocator, true)) {
        _seekLocator.SetSrc(_sourceSignature);
        indexlibv2::base::ProgressVector progress;
        progress.push_back(indexlibv2::base::Progress(_swiftParam.from, _swiftParam.to, {_startTimestamp, 0}));
        indexlibv2::base::MultiProgress multiProgress(_swiftParam.maskFilterPairs.size(), progress);
        _seekLocator.SetMultiProgress(multiProgress);
    } else {
        _seekLocator = seekLocator;
        indexlibv2::framework::Locator tmpLocator = seekLocator;
        if (!tmpLocator.ShrinkToRange(_swiftParam.from, _swiftParam.to)) {
            BS_LOG(ERROR, "seek failed, shrink failed, locator [%s], from [%d], to [%d]",
                   tmpLocator.DebugString().c_str(), _swiftParam.from, _swiftParam.to);
            return false;
        }
        if (tmpLocator.IsValid()) {
            _swiftMessageFilter.setSeekProgress(tmpLocator.GetMultiProgress());
        }
    }
    _currentLocator = _seekLocator;
    return true;
}

swift::protocol::ReaderProgress SwiftProcessedDocLocatorKeeper::getSeekProgress()
{
    auto progress = _seekLocator.GetMultiProgress();
    assert(!progress.empty());
    return util::LocatorUtil::convertLocatorMultiProgress(progress, _swiftParam.topicName, _swiftParam.maskFilterPairs,
                                                          _swiftParam.disableSwiftMaskFilter);
}

bool SwiftProcessedDocLocatorKeeper::needSkip(const indexlibv2::framework::Locator::DocInfo& docInfo)
{
    return _swiftMessageFilter.needSkip(docInfo);
}

bool SwiftProcessedDocLocatorKeeper::update(const swift::protocol::ReaderProgress& swiftProgress,
                                            const indexlibv2::framework::Locator::DocInfo& docInfo)

{
    auto [success, multiProgress] = util::LocatorUtil::convertSwiftMultiProgress(swiftProgress);
    if (!success) {
        AUTIL_LOG(ERROR, "convert swift progress failed [%s]", swiftProgress.ShortDebugString().c_str());
        return false;
    }
    _swiftMessageFilter.rewriteProgress(&multiProgress);
    if (_minTimestamp == -1) {
        _currentLocator.SetMultiProgress(multiProgress);
        return true;
    }
    indexlibv2::base::MultiProgress newProgress;
    newProgress.resize(multiProgress.size());
    for (size_t i = 0; i < multiProgress.size(); i++) {
        for (size_t j = 0; j < multiProgress[i].size(); j++) {
            const auto& progress = multiProgress[i][j];
            if (docInfo.sourceIdx == i && docInfo.hashId >= progress.from && docInfo.hashId <= progress.to) {
                // current doc progress should update
                newProgress[i].push_back(progress);
                continue;
            }
            if (progress.offset.first >= _minTimestamp) {
                const auto& subProgress =
                    getSubProgress(_currentLocator.GetMultiProgress(), i, progress.from, progress.to);
                newProgress[i].insert(newProgress[i].end(), subProgress.begin(), subProgress.end());
            } else {
                newProgress[i].push_back(progress);
            }
        }
    }
    _currentLocator.SetMultiProgress(newProgress);
    return true;
}

indexlibv2::base::ProgressVector
SwiftProcessedDocLocatorKeeper::getSubProgress(const indexlibv2::base::MultiProgress& multiProgress, uint32_t sourceIdx,
                                               uint32_t from, uint32_t to)
{
    indexlibv2::base::ProgressVector result;
    assert(sourceIdx < multiProgress.size());
    for (const auto& progress : multiProgress[sourceIdx]) {
        if (progress.to < from) {
            continue;
        }
        if (progress.from > to) {
            break;
        }
        result.emplace_back(std::max(progress.from, from), std::min(progress.to, to), progress.offset);
    }
    return result;
}

void SwiftProcessedDocLocatorKeeper::updateSchemaChangeDocInfo(const indexlibv2::framework::Locator::DocInfo& docInfo)
{
    if (_minTimestamp == -1) {
        _minTimestamp = docInfo.timestamp;
        return;
    }
    _minTimestamp = std::min(_minTimestamp, docInfo.timestamp);
}

}} // namespace build_service::workflow
