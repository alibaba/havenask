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
#include "build_service/workflow/ProcessedDocHandler.h"

#include <assert.h>
#include <cstdint>
#include <exception>
#include <string>

#include "alog/Logger.h"
#include "autil/MurmurHash.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ProcessorConfig.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/document/RawDocument.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/util/Monitor.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/indexlib.h"
#include "kmonitor/client/MetricType.h"

using autil::MurmurHash;
using autil::ScopedLock;
using autil::Thread;
using autil::ThreadPool;
using build_service::common::Locator;
using build_service::config::ProcessorConfig;
using build_service::document::ProcessedDocumentPtr;
using build_service::reader::RawDocumentReaderCreator;
using build_service::util::POP_RESULT;
using std::string;

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, ProcessedDocHandler);

ProcessedDocHandler::ProcessedDocHandler(SrcDataNode* srcNode, processor::Processor* processor)
    : _srcNode(srcNode)
    , _processor(processor)
    , _opConverter(srcNode->getConfig())
    , _lastLocator(0, 0)
    , _candidateLastLocator(0, 0)
{
}

ProcessedDocHandler::~ProcessedDocHandler() { stop(); }

bool ProcessedDocHandler::start(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                                const proto::PartitionId& partitionId,
                                indexlib::util::MetricProviderPtr metricsProvider,
                                const indexlib::util::CounterMapPtr& counterMap,
                                const indexlib::config::IndexPartitionSchemaPtr schema, int64_t initLocatorOffset)
{
    if (!_processor || !_srcNode) {
        BS_LOG(ERROR, "processor or src node is NULL");
        return false;
    }

    if (initLocatorOffset != -1) {
        common::Locator srcLocator;
        if (!getLocator(srcLocator)) {
            BS_LOG(ERROR, "cannot get src locator, processor handle start failed");
            return false;
        }
        int64_t locator =
            initLocatorOffset < srcLocator.GetOffset().first ? initLocatorOffset : srcLocator.GetOffset().first;
        _lastLocator.SetOffset({locator, 0});
        _candidateLastLocator.SetOffset({locator, 0});
    }
    BS_LOG(INFO, "doc handler init locator offset [%ld]", _lastLocator.GetOffset().first);

    ProcessorConfig processorConfig;
    config::ProcessorConfigReaderPtr processorConfigReader(
        new config::ProcessorConfigReader(resourceReader, partitionId.buildid().datatable(),
                                          getValueFromKeyValueMap(kvMap, config::PROCESSOR_CONFIG_NODE)));
    if (!processorConfigReader->getProcessorConfig("processor_config", &processorConfig)) {
        string errorMsg = "Get processor_config from [" + partitionId.buildid().datatable() + "]failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _inputQueue.reset(
        new ProcessedDocConditionQueue(processorConfig.srcQueueSize + processorConfig.processorQueueSize));
    _buildQueue.reset(new DocumentStreamQueue(processorConfig.srcQueueSize + processorConfig.processorQueueSize));
    _outputQueue.reset(
        new ProcessedDocInfoStreamQueue(processorConfig.srcQueueSize + processorConfig.processorQueueSize));
    _pool.reset(new ThreadPool(processorConfig.srcThreadNum, processorConfig.srcQueueSize));
    _input2processThread =
        Thread::createThread(std::bind(&ProcessedDocHandler::solveInputQueueDoc, this), "BsSolveInput");
    _build2outputThread = Thread::createThread(std::bind(&ProcessedDocHandler::solveWaitBuildDoc, this), "BsSolveWait");

    if (!_input2processThread || !_build2outputThread) {
        return false;
    }
    if (!_pool->start("srcWorkPool")) {
        return false;
    }
    registerMetric(metricsProvider);
    return true;
}

void ProcessedDocHandler::registerMetric(indexlib::util::MetricProviderPtr metricProvider)
{
    if (!metricProvider) {
        return;
    }
    _metricProvider = metricProvider;
    _inputQueueSizeMetric = DECLARE_METRIC(_metricProvider, "perf/srcWaitProcessCount", kmonitor::GAUGE, "count");
    _buildQueueSizeMetric = DECLARE_METRIC(_metricProvider, "perf/srcWaitBuildCount", kmonitor::GAUGE, "count");
    _outputQueueSizeMetric = DECLARE_METRIC(_metricProvider, "perf/srcWaitConsumeCount", kmonitor::GAUGE, "count");
    _processQpsMetric = DECLARE_METRIC(_metricProvider, "basic/srcProcessQps", kmonitor::QPS, "count");
    _skipQpsMetric = DECLARE_METRIC(_metricProvider, "basic/srcSkipQps", kmonitor::QPS, "count");
    _buildLocatorMetric = DECLARE_METRIC(_metricProvider, "debug/srcBuildLocator", kmonitor::STATUS, "count");
    _exceptionQpsMetric = DECLARE_METRIC(_metricProvider, "debug/srcExceptionQps", kmonitor::QPS, "count");
    _metricGroup = SrcWorkItem::SrcWorkItemMetricGroup(_processQpsMetric.get(), _buildQueueSizeMetric.get(),
                                                       _skipQpsMetric.get(), _exceptionQpsMetric.get());
}

void ProcessedDocHandler::solveInputQueueDoc()
{
    while (true) {
        DocInfo doc;
        ProcessedDocumentPtr processedDoc;
        POP_RESULT ret;
        while (true) {
            {
                ScopedLock lock(_pkMutex);
                ret = _inputQueue->pop(doc, [this](const DocInfo& docInfo) {
                    return _ongoingPks.find(docInfo.pkHash) == _ongoingPks.end();
                });
                if (ret == POP_RESULT::PR_OK) {
                    _ongoingPks.insert(doc.pkHash);
                } else if (ret == POP_RESULT::PR_NOVALID) {
                    _pkMutex.wait();
                    continue;
                } else if (ret == POP_RESULT::PR_EOF) {
                    return;
                }
            }
            REPORT_METRIC(_inputQueueSizeMetric, _inputQueue->size());
            SrcWorkItem* item = new SrcWorkItem(doc.doc, _srcNode, &_opConverter, _buildQueue.get(), &_metricGroup);
            _pool->pushWorkItem((autil::WorkItem*)item);
        }
    }
}

bool ProcessedDocHandler::runWithTry(std::function<void(void)> func, uint64_t retryTime)
{
    for (uint32_t i = 0; i < retryTime; ++i) {
        try {
            func();
            return true;
        } catch (const std::exception& e) {
            INCREASE_QPS(_exceptionQpsMetric);
        }
    }
    return false;
}

void ProcessedDocHandler::processBuiltDoc(const ProcessedDocumentPtr& doc, const SourceOpConverter::SolvedDocType& type)
{
    int64_t docTs = INVALID_TIMESTAMP;
    if (doc->getLegacyDocument()) {
        docTs = doc->getLegacyDocument()->GetTimestamp();
    }
    switch (type) {
    case SourceOpConverter::SDT_FAILED_DOC:
    case SourceOpConverter::SDT_ORDER_PRESERVE_FAILED:
    case SourceOpConverter::SDT_PROCESSED_DOC:
        _outputQueue->push({doc, type, docTs});
        break;
    case SourceOpConverter::SDT_UPDATE_FAILED: {
        _outputQueue->push({doc, SourceOpConverter::SDT_UNPROCESSED_DOC, docTs});
        document::RawDocumentPtr rawDoc(new indexlib::document::DefaultRawDocument);
        _srcNode->toRawDocument(doc->getLegacyDocument(), rawDoc);
        rawDoc->setField(document::HA_RESERVED_SKIP_REASON, document::UPDATE_FAILED);
        _processor->processDoc(rawDoc);
        break;
    }

    case SourceOpConverter::SDT_UNPROCESSED_DOC: {
        _outputQueue->push({doc, type, docTs});
        // TODO xiaohao.yxh support custom raw doc
        document::RawDocumentPtr rawDoc(new indexlib::document::DefaultRawDocument);
        _srcNode->toRawDocument(doc->getLegacyDocument(), rawDoc);
        _processor->processDoc(rawDoc);
        break;
    }

    default:
        BS_LOG(ERROR, "unknow type");
        assert(false);
    }
}

void ProcessedDocHandler::solveWaitBuildDoc()
{
    uint64_t retryTime = 0;
    if (_srcNode) {
        config::SrcNodeConfig* srcConfig = _srcNode->getConfig();
        assert(srcConfig);
        retryTime = srcConfig->exceptionRetryTime;
    }
    while (true) {
        std::pair<ProcessedDocumentPtr, SourceOpConverter::SolvedDocType> docInfo;
        if (!_buildQueue->pop(docInfo)) {
            return;
        }
        auto& doc = docInfo.first;
        assert(doc);
        auto originalLocator = doc->getLocator();
        doc->setLocator(_lastLocator);
        if (doc->getLegacyDocument()) {
            doc->getLegacyDocument()->SetLocator(_lastLocator);
        }
        if (docInfo.second != SourceOpConverter::SDT_FAILED_DOC &&
            !runWithTry(
                [srcNode = _srcNode, &doc]() mutable {
                    if (doc->getLegacyDocument()) {
                        if (!srcNode->build(doc->getLegacyDocument())) {
                            BS_LOG(ERROR, "build document pk [%s] failed",
                                   doc->getLegacyDocument()->GetPrimaryKey().c_str());
                        }
                    } else {
                        BS_LOG(WARN, "can not get document");
                    }
                },
                retryTime)) {
            _outputQueue->push({nullptr, SourceOpConverter::SDT_FAILED_DOC, INVALID_TIMESTAMP});
            continue;
        } else {
            processBuiltDoc(doc, docInfo.second);
        }
        if (docInfo.second == SourceOpConverter::SDT_FAILED_DOC) {
            continue;
        }
        {
            ScopedLock lock(_locatorMutex);
            auto iter = _uncommitedLocators.find(originalLocator);
            assert(iter != _uncommitedLocators.end());
            if (--(iter->second) == 0) {
                if (iter == _uncommitedLocators.begin()) {
                    _candidateLastLocator = iter->first;
                }
                _uncommitedLocators.erase(iter);
            }
        }
        uint64_t pkHash = 0;
        if (doc->getLegacyDocument()) {
            const string& pk = doc->getLegacyDocument()->GetPrimaryKey();
            pkHash = MurmurHash::MurmurHash64A(pk.c_str(), pk.length(), 0);
        }
        ScopedLock lock(_pkMutex);
        _ongoingPks.erase(pkHash);
        _pkMutex.signal();
    }
}

void ProcessedDocHandler::consume(ProcessedDocumentPtr& processedDoc)
{
    {
        Locator locator = processedDoc->getLocator();
        ScopedLock lock(_locatorMutex);
        if (locator.GetOffset() > _candidateLastLocator.GetOffset()) {
            _lastLocator = _candidateLastLocator;
        }
        if (_uncommitedLocators.find(locator) == _uncommitedLocators.end()) {
            _uncommitedLocators[locator] = 0;
        }
        _uncommitedLocators[locator]++;
    }
    uint64_t pkHash = 0;
    if (processedDoc->getLegacyDocument()) {
        // do not have document, pkHash = 0, skip doc
        const string& pk = processedDoc->getLegacyDocument()->GetPrimaryKey();
        pkHash = MurmurHash::MurmurHash64A(pk.c_str(), pk.length(), 0);
    }
    _inputQueue->push({processedDoc, pkHash});
    {
        ScopedLock lock(_pkMutex);
        _pkMutex.signal();
    }
}

FlowError ProcessedDocHandler::getProcessedDoc(ProcessedDocumentPtr& doc, int64_t& originalDocTs)
{
    DocInfo docInfo;
    bool ret = _outputQueue->pop(docInfo);
    if (!ret) {
        return FE_EOF;
    }
    REPORT_METRIC(_outputQueueSizeMetric, _outputQueue->size());
    if (docInfo.type == SourceOpConverter::SDT_FAILED_DOC) {
        stop(true);
        return FE_FATAL;
    } else if (docInfo.type == SourceOpConverter::SDT_UNPROCESSED_DOC) {
        doc = (*_processor->getProcessedDoc())[0];
    } else {
        doc = docInfo.doc;
    }
    originalDocTs = docInfo.docTimestamp;
    return FE_OK;
}

bool ProcessedDocHandler::getLocator(Locator& locator) const { return _srcNode->getLocator(locator); }

void ProcessedDocHandler::stop(bool instant)
{
    BS_LOG(INFO, "handler stop , instant [%d]", (int32_t)instant);
    if (_inputQueue) {
        _inputQueue->setFinish();
        if (instant) {
            _inputQueue->setForceStop();
            _inputQueue->clear();
        }
    }
    {
        ScopedLock lock(_pkMutex);
        _pkMutex.signal();
    }
    _input2processThread.reset();
    if (_pool) {
        _pool->stop(instant ? ThreadPool::STOP_AND_CLEAR_QUEUE : ThreadPool::STOP_AFTER_QUEUE_EMPTY);
    }
    if (_buildQueue) {
        _buildQueue->setFinish();
        if (instant) {
            _buildQueue->clear();
        }
    }
    _build2outputThread.reset();
    if (_processor) {
        _processor->stop(instant);
    }
    if (_outputQueue) {
        _outputQueue->setFinish();
    }
}
}} // namespace build_service::workflow
