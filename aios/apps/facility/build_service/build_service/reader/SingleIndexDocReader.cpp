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
#include "build_service/reader/SingleIndexDocReader.h"

#include "indexlib/index/inverted_index/AndPostingExecutor.h"
#include "indexlib/index/inverted_index/DocidRangePostingExecutor.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/OrPostingExecutor.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermPostingExecutor.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SingleIndexDocReader);

SingleIndexDocReader::SingleIndexDocReader() : _currentDocId(INVALID_DOCID), _ignoreReadError(false) {}

SingleIndexDocReader::~SingleIndexDocReader() {}

bool SingleIndexDocReader::init(const ReaderInitParam& param, int64_t offset)
{
    if (param.indexVersion == INVALID_VERSION) {
        AUTIL_LOG(ERROR, "invalid version id [%d]", param.indexVersion);
        return false;
    }

    _ignoreReadError = param.ignoreReadError;
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    OfflineConfig& offlineConfig = options.GetOfflineConfig();
    offlineConfig.readerConfig.loadIndex = false;
    offlineConfig.readerConfig.summaryCacheSize = param.blockCacheSize;
    offlineConfig.readerConfig.indexCacheSize = param.blockCacheSize;
    offlineConfig.readerConfig.cacheBlockSize = param.cacheBlockSize;

    // use legacy branch name policy,
    _indexPart.reset(new OfflinePartition(/*branch name legacy*/ true));
    auto ret = _indexPart->Open(param.indexRootPath, "", /*schema*/ nullptr, options, param.indexVersion);
    if (ret != IndexPartition::OS_OK) {
        AUTIL_LOG(ERROR, "open offline partition fail, root [%s], version [%d]", param.indexRootPath.c_str(),
                  param.indexVersion);
        return false;
    }

    if (param.readRange.first > param.readRange.second || param.indexRange.first > param.indexRange.second ||
        param.readRange.first < param.indexRange.first || param.readRange.second > param.indexRange.second) {
        AUTIL_LOG(ERROR,
                  "invalid parameter: read range [%u, %u] "
                  "should be in range [%u, %u]",
                  param.readRange.first, param.readRange.second, param.indexRange.first, param.indexRange.second);
        return false;
    }

    IndexPartitionReaderPtr reader = _indexPart->GetReader();
    AUTIL_LOG(INFO, "partition reader version [%d]", reader->GetVersion().GetVersionId());
    PartitionInfoPtr partInfo = reader->GetPartitionInfo();
    size_t totalDocCount = partInfo->GetTotalDocCount();
    _targetRange = splitDocIdRange(totalDocCount, param.readRange, param.indexRange);
    if (!initPostingExecutor(_targetRange, param.userDefineIndexParam)) {
        return false;
    }
    if (!seek(offset)) {
        return false;
    }
    if (_currentDocId == _targetRange.second) {
        AUTIL_LOG(INFO, "init offset [%ld] reach eof, currentDocId [%ld].", offset, _currentDocId);
        return true;
    }
    _rawDocExtractor.reset(new indexlib::partition::RawDocumentFieldExtractor(param.requiredFields));
    if (!_rawDocExtractor->Init(reader, param.preferSourceIndex)) {
        AUTIL_LOG(ERROR, "init RawDocumentFieldExtractor fail");
        return false;
    }
    return true;
}

bool SingleIndexDocReader::read(document::RawDocument& doc)
{
    while (true) {
        if (isEof()) {
            return false;
        }

        SeekStatus status = _rawDocExtractor->Seek(_currentDocId);
        if (status == SS_ERROR) {
            AUTIL_LOG(ERROR, "read current docid [%ld] fail.", _currentDocId);
            if (_ignoreReadError) {
                if (!seek(_currentDocId + 1)) {
                    return false;
                }
                continue;
            }
            return false;
        }

        if (status == SS_DELETED) {
            if (!seek(_currentDocId + 1)) {
                return false;
            }
            continue;
        }

        assert(status == SS_OK);
        RawDocumentFieldExtractor::FieldIterator fieldIter = _rawDocExtractor->CreateIterator();
        fieldIter.ToRawDocument(doc);
        return true;
    }
    return false;
}

bool SingleIndexDocReader::seek(int64_t offset)
{
    if (offset < _targetRange.first) {
        AUTIL_LOG(INFO, "target offset less than docid begin [%ld], change to begin docId [%ld] by default.", offset,
                  _targetRange.first);
        offset = _targetRange.first;
    }

    if (offset > _targetRange.second) {
        AUTIL_LOG(INFO, "target offset greater than docid end [%ld], change to end docId [%ld] by default.", offset,
                  _targetRange.second);
        offset = _targetRange.second;
    }

    docid_t docid = INVALID_DOCID;
    try {
        docid = _postingExecutor->Seek((docid_t)offset);
    } catch (const FileIOException& e) {
        AUTIL_LOG(ERROR, "Seek docId [%ld] failed. Caught file io exception: %s", offset, e.what());
        return false;
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "Seek docId[%ld] failed. Caught exception: %s", offset, e.what());
        return false;
    } catch (const exception& e) {
        AUTIL_LOG(ERROR, "Seek docId[%ld] failed. Caught std exception: %s", offset, e.what());
        return false;
    } catch (...) {
        IE_LOG(ERROR, "Seek docId[%ld] failed. Caught unknown exception", offset);
        return false;
    }

    if (docid == indexlib::END_DOCID) {
        AUTIL_LOG(INFO, "target offset [%ld] reach eof.", offset);
        _currentDocId = _targetRange.second;
        _rawDocExtractor.reset();
        return true;
    }
    _currentDocId = docid;
    return true;
}

pair<int64_t, int64_t> SingleIndexDocReader::splitDocIdRange(size_t totalDocCount, pair<uint32_t, uint32_t> readRange,
                                                             pair<uint32_t, uint32_t> indexRange) const
{
    // read doc = pre[readRange.from-1]+1 ~ pre[readRange.to]
    int64_t totalRange = indexRange.second - indexRange.first + 1;
    int64_t begin = totalDocCount * (readRange.first - indexRange.first) / totalRange;
    int64_t end = totalDocCount * (readRange.second - indexRange.first + 1) / totalRange;
    return make_pair(begin, end);
    // docRange [begin, end)
}

bool SingleIndexDocReader::initPostingExecutor(const pair<int64_t, int64_t>& targetRange,
                                               const vector<IndexQueryCondition>& userDefineParam)
{
    BS_LOG(INFO, "init posting executor with range [%ld, %ld)", targetRange.first, targetRange.second);
    _postingExecutor.reset(new DocidRangePostingExecutor(targetRange.first, targetRange.second));
    if (!userDefineParam.empty()) {
        shared_ptr<PostingExecutor> udfExecutor = createUserDefinePostingExecutor(userDefineParam);
        if (!udfExecutor) {
            AUTIL_LOG(ERROR, "create user define executor error with parameter [%s].",
                      ToJsonString(userDefineParam, true).c_str());
            return false;
        }
        vector<shared_ptr<PostingExecutor>> innerExecutors;
        innerExecutors.push_back(udfExecutor);
        innerExecutors.push_back(_postingExecutor);
        _postingExecutor.reset(new AndPostingExecutor(innerExecutors));
    }
    return true;
}

shared_ptr<PostingExecutor>
SingleIndexDocReader::createUserDefinePostingExecutor(const vector<IndexQueryCondition>& userDefineParam) const
{
    assert(!userDefineParam.empty());
    assert(_indexPart);
    IndexPartitionReaderPtr reader = _indexPart->GetReader();

    vector<shared_ptr<PostingExecutor>> innerExecutor;
    for (size_t i = 0; i < userDefineParam.size(); i++) {
        shared_ptr<PostingExecutor> executor = createSinglePostingExecutor(reader, userDefineParam[i]);
        if (!executor) {
            AUTIL_LOG(ERROR, "create posting executor for single condition [%s] fail.",
                      ToJsonString(userDefineParam[i], true).c_str());
            return shared_ptr<PostingExecutor>();
        }
        innerExecutor.push_back(executor);
    }

    if (innerExecutor.size() == 1) {
        return innerExecutor[0];
    }
    shared_ptr<PostingExecutor> executor(new OrPostingExecutor(innerExecutor));
    return executor;
}

shared_ptr<PostingExecutor>
SingleIndexDocReader::createSinglePostingExecutor(const indexlib::partition::IndexPartitionReaderPtr& reader,
                                                  const IndexQueryCondition& indexQueryCondition) const
{
    const string& conditionType = indexQueryCondition.GetConditionType();
    const vector<IndexTermInfo>& termInfos = indexQueryCondition.GetIndexTermInfos();

    if (termInfos.empty()) {
        AUTIL_LOG(ERROR, "invalid indexQueryCondition with empty term infos");
        return shared_ptr<PostingExecutor>();
    }

    if (conditionType == IndexQueryCondition::TERM_CONDITION_TYPE || termInfos.size() == 1) {
        return createTermPostingExecutor(reader, termInfos[0]);
    }

    vector<shared_ptr<PostingExecutor>> innerExecutor;
    for (size_t i = 0; i < termInfos.size(); i++) {
        shared_ptr<PostingExecutor> executor = createTermPostingExecutor(reader, termInfos[i]);
        if (!executor) {
            return shared_ptr<PostingExecutor>();
        }
        innerExecutor.push_back(executor);
    }

    if (conditionType == IndexQueryCondition::AND_CONDITION_TYPE) {
        return shared_ptr<PostingExecutor>(new AndPostingExecutor(innerExecutor));
    }
    if (conditionType == IndexQueryCondition::OR_CONDITION_TYPE) {
        return shared_ptr<PostingExecutor>(new OrPostingExecutor(innerExecutor));
    }
    AUTIL_LOG(ERROR, "invalid conditionType [%s].", conditionType.c_str());
    return shared_ptr<PostingExecutor>();
}

shared_ptr<PostingExecutor>
SingleIndexDocReader::createTermPostingExecutor(const indexlib::partition::IndexPartitionReaderPtr& reader,
                                                const IndexTermInfo& termInfo) const
{
    assert(reader);
    auto indexReader = reader->GetInvertedIndexReader(termInfo.indexName);
    if (!indexReader) {
        AUTIL_LOG(ERROR, "create term posting executor for index [%s] fail.", termInfo.indexName.c_str());
        return shared_ptr<PostingExecutor>();
    }

    indexlib::index::Term term(termInfo.term, termInfo.indexName);
    auto iterResult = indexReader->Lookup(term);
    if (!iterResult.Ok()) {
        AUTIL_LOG(ERROR, "look up term [%s] for index [%s] failed, error code [%d].", termInfo.term.c_str(),
                  termInfo.indexName.c_str(), (int32_t)iterResult.GetErrorCode());
        return shared_ptr<PostingExecutor>();
    }
    std::shared_ptr<PostingIterator> iter(iterResult.Value());
    if (!iter) {
        AUTIL_LOG(INFO, "term [%s] does not exist in index [%s]", termInfo.term.c_str(), termInfo.indexName.c_str());
        return shared_ptr<PostingExecutor>(new DocidRangePostingExecutor(0, 0));
    }
    return shared_ptr<PostingExecutor>(new TermPostingExecutor(iter));
}

}} // namespace build_service::reader
