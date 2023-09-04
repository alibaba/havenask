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
#include "trace_server/TraceServerWorker.h"

#include "absl/flags/flag.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"

// work mode related flags.
ABSL_FLAG(std::string, swiftRoot, "swiftRoot", "swiftRoot address to read from");
ABSL_FLAG(std::string, topicName, "topicName", "topic name");
ABSL_FLAG(std::string, range, "0-65535", "read swift partition range");
ABSL_FLAG(int64_t, docCount, 10000, "read doc count, default read 10000");
ABSL_FLAG(int64_t, beginTimestamp, 0, "read doc from beginTimestamp, default read from 0");
ABSL_FLAG(int64_t, endTimestamp, 10000000000000000, "read doc from endTimestamp, default read to current time");
ABSL_FLAG(std::string, schemaPath, "", "when read processed topic, need add schema path");
ABSL_FLAG(std::string, requiredFields, "", "required fields, example: f1;f2");
ABSL_FLAG(bool, needFieldFilter, false, "need field filter");

namespace build_service::tools {
AUTIL_DECLARE_AND_SETUP_LOGGER(tools, TraceServerWorker);

struct TraceServerWorker::Impl {
    util::SwiftClientCreatorPtr swiftClientCreator; // TODO: reset per day through drogo?
};

TraceServerWorker::TraceServerWorker() : _impl(std::make_unique<TraceServerWorker::Impl>()) {}

TraceServerWorker::~TraceServerWorker() {}

static std::string toJsonString(const ::google::protobuf::Message* message)
{
    autil::legacy::json::JsonMap jsonMap;
    http_arpc::ProtoJsonizer::toJsonMap(*message, jsonMap);
    std::string retString = autil::legacy::ToJsonString(jsonMap, true);
    return retString;
}

void TraceServerWorker::init() { _impl->swiftClientCreator = std::make_shared<util::SwiftClientCreator>(); }

template <typename ReadSwiftRequest>
static bool fillSwiftRequestFromArgs(ReadSwiftRequest* request)
{
    request->set_swiftroot(absl::GetFlag(FLAGS_swiftRoot));
    request->set_topicname(absl::GetFlag(FLAGS_topicName));
    request->set_swiftreaderconfig("oneRequestReadCount=1000;readBufferSize=50000;decompress=true");
    if (absl::GetFlag(FLAGS_needFieldFilter)) {
        request->set_needfieldfilter("true");
    } else {
        request->set_needfieldfilter("false");
    }
    auto range = absl::GetFlag(FLAGS_range);
    std::vector<uint32_t> rangeNum;
    autil::StringUtil::fromString(range, rangeNum, std::string("-"));
    if (rangeNum.size() != 2) {
        AUTIL_LOG(ERROR, "invalid range [%s]", range.c_str());
        return false;
    }
    request->set_from(rangeNum[0]);
    request->set_to(rangeNum[1]);
    request->set_starttimestamp(absl::GetFlag(FLAGS_beginTimestamp));
    request->set_stoptimestamp(absl::GetFlag(FLAGS_endTimestamp));
    request->set_readlimit(absl::GetFlag(FLAGS_docCount));

    return true;
}

static std::unique_ptr<reader::RawDocumentReader> createDocumentReader(const reader::ReaderInitParam& readerInitParam,
                                                                       reader::RawDocumentReaderCreator& readerCreator,
                                                                       proto::ReadSwiftResponse* response)
{
    std::unique_ptr<reader::RawDocumentReader> documentReader(
        readerCreator.create(readerInitParam.resourceReader, readerInitParam.kvMap, readerInitParam.partitionId,
                             readerInitParam.metricProvider, readerInitParam.counterMap, readerInitParam.schema));
    if (!documentReader) {
        AUTIL_LOG(ERROR, "Create RawDocumentReader FAILED.");
        response->set_errormsg("Create RawDocumentReader FAILED.");
        response->set_errorcode(proto::ErrorCode::ERROR_SWIFT_INIT);
        return nullptr;
    }
    return documentReader;
}

static std::pair<indexlib::document::RawDocumentPtr, reader::Checkpoint>
readOneRawDocument(const std::unique_ptr<reader::RawDocumentReader>& documentReader, proto::ReadSwiftResponse* response)
{
    indexlib::document::RawDocumentPtr document;
    reader::Checkpoint checkpoint;
    auto ec = documentReader->read(document, &checkpoint);
    // if ((ec != reader::RawDocumentReader::ERROR_NONE && ec != reader::RawDocumentReader::ERROR_WAIT) || !document) {
    while (ec == reader::RawDocumentReader::ERROR_WAIT) {
        AUTIL_LOG(INFO, "error wait continue read");
        ec = documentReader->read(document, &checkpoint);
    }
    if (ec != reader::RawDocumentReader::ERROR_NONE || !document) {
        response->set_errormsg("Read document ERROR[" + autil::legacy::ToJsonString(ec) + "].");
        response->set_errorcode(proto::ErrorCode::ERROR_SWIFT_READ);
        return {nullptr, checkpoint};
    }
    return {document, checkpoint};
}

static void fillOneRawDocument(const indexlib::document::RawDocument* rawDocument, proto::Document* document)
{
    document->set_swifthashid(rawDocument->GetTag("swift_hashId"));
    document->set_swiftmsgtime(rawDocument->GetTag("swift_msgTime"));
    document->set_swiftmsgid(rawDocument->GetTag("swift_msgId"));
    document->set_swiftoffset(rawDocument->GetTag("swift_offset"));
}

static bool filterOneDefaultRawDocument(const indexlib::document::DefaultRawDocument* defaultRawDocument,
                                        const std::map<autil::StringView, autil::StringView>& filter)
{
    if (filter.empty() || !defaultRawDocument) {
        return false;
    }
    std::unique_ptr<indexlib::document::RawDocFieldIterator> rawDocFieldIterator(defaultRawDocument->CreateIterator());
    assert(rawDocFieldIterator);
    while (rawDocFieldIterator->IsValid()) {
        auto iter = filter.find(rawDocFieldIterator->GetFieldName());
        if ((iter != filter.end()) && (iter->second != rawDocFieldIterator->GetFieldValue())) {
            return true;
        }
        rawDocFieldIterator->MoveToNext();
    }
    return false;
}

static void fillOneDefaultRawDocument(const indexlib::document::DefaultRawDocument* defaultRawDocument,
                                      proto::Document* document)
{
    if (!defaultRawDocument) {
        return;
    }
    std::unique_ptr<indexlib::document::RawDocFieldIterator> rawDocFieldIterator(defaultRawDocument->CreateIterator());
    assert(rawDocFieldIterator);
    while (rawDocFieldIterator->IsValid()) {
        *document->add_names() = rawDocFieldIterator->GetFieldName();
        *document->add_values() = rawDocFieldIterator->GetFieldValue();
        rawDocFieldIterator->MoveToNext();
    }
}

static void fillOneKVDocument(const indexlib::document::KVDocumentPtr& kvDocument,
                              const std::map<autil::StringView, autil::StringView>& filter,
                              proto::ReadSwiftResponse* response)
{
    if (filterOneDefaultRawDocument(kvDocument->_rawDoc.get(), filter)) {
        return;
    }

    // KVDocument --> vecotr<vector<pair>>
    std::vector<std::vector<std::pair<std::string, std::string>>> kvDocs;
    for (auto it = kvDocument->begin(); it != kvDocument->end(); ++it) {
        auto& kvDoc = kvDocs.emplace_back();
        kvDoc.emplace_back("__kvDoc:CMD", autil::legacy::ToJsonString(it->GetDocOperateType()));
        kvDoc.emplace_back("__kvDoc:pkeyHash", autil::legacy::ToJsonString(it->GetPKeyHash()));
        kvDoc.emplace_back("__kvDoc:skeyHash", autil::legacy::ToJsonString(it->GetSKeyHash()));
        kvDoc.emplace_back("__kvDoc:value", it->GetValue());
        kvDoc.emplace_back("__kvDoc:timestamp", autil::legacy::ToJsonString(it->GetTimestamp()));
        kvDoc.emplace_back("__kvDoc:userTimestamp", autil::legacy::ToJsonString(it->GetUserTimestamp()));
        kvDoc.emplace_back("__kvDoc:TTL", autil::legacy::ToJsonString(it->GetTTL()));
        kvDoc.emplace_back(it->GetPkFieldName(), it->GetPkFieldValue());
    }

    // filter KVIndexDocument
    std::set<size_t> filteredDocs;
    if (!filter.empty()) {
        for (size_t i = 0; i < kvDocs.size(); ++i) {
            for (size_t j = 0; j < kvDocs[i].size(); ++j) {
                const auto& [name, value] = kvDocs[i][j];
                auto iter = filter.find(name);
                if (iter != filter.end() && iter->second != value) {
                    filteredDocs.insert(i); // filter doc[i]
                    break;
                }
            }
        }
    }
    if (kvDocs.size() != 0 && filteredDocs.size() == kvDocs.size()) {
        return; // all by filter
    }

    // fill KVDocument
    auto* retDoc = response->add_documents();
    fillOneRawDocument(kvDocument.get(), retDoc);
    fillOneDefaultRawDocument(kvDocument->_rawDoc.get(), retDoc);
    for (size_t i = 0; i < kvDocs.size(); ++i) {
        if (filteredDocs.count(i) > 0) {
            continue;
        }
        // fill KVIndexDocument
        auto* subDoc = retDoc->add_documents();
        for (const auto& [name, value] : kvDocs[i]) {
            *subDoc->add_names() = name;
            *subDoc->add_values() = value;
        }
    }
}

static void fillFinializeInfo(size_t readCount, indexlib::document::RawDocumentPtr lastDocument,
                              proto::ReadSwiftResponse* response)
{
    response->set_readcount(readCount);
    response->set_matchcount(response->documents_size());
    if (lastDocument) {
        response->mutable_lastdocument()->set_swifthashid(lastDocument->GetTag("swift_hashId"));
        response->mutable_lastdocument()->set_swiftmsgtime(lastDocument->GetTag("swift_msgTime"));
        response->mutable_lastdocument()->set_swiftmsgid(lastDocument->GetTag("swift_msgId"));
        response->mutable_lastdocument()->set_swiftoffset(lastDocument->GetTag("swift_offset"));
    }
    if (!response->has_errorcode()) {
        response->set_errorcode(proto::ErrorCode::ERROR_NONE);
    }
}

template <typename ReadSwiftRequest>
static std::map<autil::StringView, autil::StringView> createFilter(const ReadSwiftRequest* request)
{
    std::map<autil::StringView, autil::StringView> filter;
    for (size_t i = 0; i < request->filter_size(); ++i) {
        const auto& field = request->filter(i);
        filter[field.name()] = field.value();
    }
    return filter;
}

template <typename ReadSwiftRequest>
static void readKVDocument(const std::unique_ptr<reader::RawDocumentReader>& documentReader,
                           const ReadSwiftRequest* request, proto::ReadSwiftResponse* response)
{
    if (!documentReader) {
        return;
    }

    // process
    auto filter = createFilter(request);
    size_t readCount = 0;
    indexlib::document::RawDocumentPtr lastDocument;
    for (size_t idx = 0; idx < request->readlimit(); ++idx) {
        auto [document, checkpoint] = readOneRawDocument(documentReader, response);
        if (!document) {
            break;
        }
        ++readCount;
        lastDocument = document;
        auto kvDocument = std::dynamic_pointer_cast<indexlib::document::KVDocument>(document);
        if (!kvDocument) {
            response->set_errormsg("Cast to KVDocument FAILED.");
            response->set_errorcode(proto::ErrorCode::ERROR_DOCUMENT_PARSE);
            break;
        }

        fillOneKVDocument(kvDocument, filter, response);

        if (request->stoptimestamp() >= 0 && checkpoint.offset > request->stoptimestamp()) {
            response->set_errormsg("Stop, offset[" + autil::legacy::ToJsonString(checkpoint.offset) +
                                   "] > stopTimeStamp[" + autil::legacy::ToJsonString(request->stoptimestamp()) + "].");
            response->set_errorcode(proto::ErrorCode::ERROR_STOP_TIMESTAMP);
            break;
        }
    }

    fillFinializeInfo(readCount, lastDocument, response);
}

template <typename ReadSwiftRequest>
static void readDefaultRawDocument(const std::unique_ptr<reader::RawDocumentReader>& documentReader,
                                   const ReadSwiftRequest* request, proto::ReadSwiftResponse* response)
{
    if (!documentReader) {
        return;
    }
    // process
    auto filter = createFilter(request);
    size_t readCount = 0;
    indexlib::document::RawDocumentPtr lastDocument;
    for (size_t idx = 0; idx < request->readlimit(); ++idx) {
        auto [rawDocument, checkpoint] = readOneRawDocument(documentReader, response);
        if (!rawDocument) {
            break;
        }
        ++readCount;
        lastDocument = rawDocument;
        auto defaultRawDocument = std::dynamic_pointer_cast<indexlib::document::DefaultRawDocument>(rawDocument);
        if (!defaultRawDocument) {
            response->set_errormsg("Cast to DefaultRawDocument FAILED.");
            response->set_errorcode(proto::ErrorCode::ERROR_DOCUMENT_PARSE);
            break;
        }

        if (!filterOneDefaultRawDocument(defaultRawDocument.get(), filter)) {
            auto* retDoc = response->add_documents();
            fillOneRawDocument(rawDocument.get(), retDoc);
            fillOneDefaultRawDocument(defaultRawDocument.get(), retDoc);
        }

        if (request->stoptimestamp() >= 0 && checkpoint.offset > request->stoptimestamp()) {
            response->set_errormsg("Stop, offset[" + autil::legacy::ToJsonString(checkpoint.offset) +
                                   "] > stopTimeStamp[" + autil::legacy::ToJsonString(request->stoptimestamp()) + "].");
            response->set_errorcode(proto::ErrorCode::ERROR_STOP_TIMESTAMP);
            break;
        }
    }

    fillFinializeInfo(readCount, lastDocument, response);
}

static bool isRequired(const std::string& name, const std::vector<std::string>& requiredFields)
{
    if (requiredFields.size() == 0) {
        return true;
    }
    for (const auto& field : requiredFields) {
        if (name == field) {
            return true;
        }
    }
    return false;
}

template <typename ReadRequest>
static void readAndPrintDocs(ReadRequest request, const reader::ReaderInitParam& readerInitParam,
                             util::SwiftClientCreatorPtr clientCreator)
{
    auto requiredFieldsStr = absl::GetFlag(FLAGS_requiredFields);
    std::vector<std::string> requiredFields;
    autil::StringUtil::fromString(requiredFieldsStr, requiredFields, std::string(";"));
    proto::ReadSwiftResponse response;
    reader::RawDocumentReaderCreator readerCreator(clientCreator);
    auto documentReader = createDocumentReader(readerInitParam, readerCreator, &response);
    int32_t readLeft = request.readlimit();
    while (readLeft > 0) {
        proto::ReadSwiftResponse response;
        if (readLeft < 100) {
            request.set_readlimit(readLeft);
            readLeft = 0;
        } else {
            readLeft -= 100;
            request.set_readlimit(100);
        }
        if (readerInitParam.schema->GetTableType() == indexlib::tt_kv ||
            readerInitParam.schema->GetTableType() == indexlib::tt_kkv) {
            readKVDocument(documentReader, &request, &response);
        } else {
            readDefaultRawDocument(documentReader, &request, &response);
        }
        if (response.errorcode() != proto::ErrorCode::ERROR_NONE &&
            response.errorcode() != proto::ErrorCode::ERROR_STOP_TIMESTAMP) {
            AUTIL_LOG(ERROR, "read swift failed, error info [%s]", response.errormsg().c_str());
            return;
        }
        for (const auto& document : response.documents()) {
            AUTIL_LOG(INFO, "=================");
            AUTIL_LOG(INFO, "swiftMsgId: %s, swiftMsgTime: %s, swiftHashId: %s, swiftOffset: %s",
                      document.swiftmsgid().c_str(), document.swiftmsgtime().c_str(), document.swifthashid().c_str(),
                      document.swiftoffset().c_str());
            for (int i = 0; i < document.names_size(); i++) {
                const auto& name = document.names(i);
                if (isRequired(name, requiredFields)) {
                    AUTIL_LOG(INFO, "%s : %s", document.names(i).c_str(), document.values(i).c_str());
                }
            }
        }
        if (response.errorcode() == proto::ErrorCode::ERROR_STOP_TIMESTAMP) {
            AUTIL_LOG(INFO, "read eof exit");
            return;
        }
    }
}

template <typename ReadSwiftRequest>
static build_service::reader::ReaderInitParam createReaderInitParam(const ReadSwiftRequest* request)
{
    reader::ReaderInitParam readerInitParam;
    build_service::KeyValueMap keyValueMap;
    keyValueMap[build_service::config::SWIFT_ZOOKEEPER_ROOT] = request->swiftroot();
    keyValueMap[build_service::config::SWIFT_TOPIC_NAME] = request->topicname();
    keyValueMap[build_service::config::SWIFT_CLIENT_CONFIG] = request->swiftclientconfig();
    keyValueMap[build_service::config::SWIFT_READER_CONFIG] = request->swiftreaderconfig();
    keyValueMap[build_service::config::RAW_TOPIC_SWIFT_READER_CONFIG] = request->swiftfilterconfig();
    keyValueMap[build_service::config::USE_FIELD_SCHEMA] = request->usefieldschema();
    keyValueMap[build_service::config::NEED_FIELD_FILTER] = request->needfieldfilter();
    keyValueMap[build_service::config::SWIFT_TOPIC_STREAM_MODE] = "false";
    keyValueMap[build_service::config::NEED_PRINT_DOC_TAG] = "true";
    if (request->starttimestamp() >= 0) {
        keyValueMap[build_service::config::SWIFT_START_TIMESTAMP] =
            autil::StringUtil::toString(request->starttimestamp());
    }
    // keyValueMap[build_service::reader::RAW_DOCUMENT_FORMAT] =
    // build_service::reader::RAW_DOCUMENT_FORMAT_SWIFT_FILED_FILTER;
    readerInitParam.kvMap = keyValueMap;
    // readerInitParam.resourceReader = config::ResourceReaderManager::getResourceReader(configPath);
    readerInitParam.metricProvider = indexlib::util::MetricProviderPtr();
    readerInitParam.partitionId.mutable_range()->set_from(request->from());
    readerInitParam.partitionId.mutable_range()->set_to(request->to());
    *readerInitParam.partitionId.add_clusternames() =
        ""; // HACK: to avoid except in TABLE_LOG, caused by access clusternames[0]
    *readerInitParam.partitionId.add_clusternames() =
        ""; // HACK: to avoid core in RawDocumentReader::initBatchBuil caused by null resourceReader
    return readerInitParam;
}

void build_service::tools::TraceServerWorker::run()
{
    auto schemaPath = absl::GetFlag(FLAGS_schemaPath);
    if (!schemaPath.empty()) {
        proto::ReadProcessedSwiftRequest request;
        proto::ReadSwiftResponse response;
        if (!fillSwiftRequestFromArgs(&request)) {
            return;
        }
        request.set_schemapath(schemaPath);
        AUTIL_LOG(INFO, "read param:[%s]", request.DebugString().c_str());
        // prepare readerInitParam
        auto readerInitParam = createReaderInitParam(&request);
        readerInitParam.kvMap[build_service::config::READ_SRC_TYPE] = build_service::config::SWIFT_PROCESSED_READ_SRC;
        readerInitParam.kvMap["print_attribute"] = "true";
        readerInitParam.kvMap["print_summary"] = "true";
        readerInitParam.kvMap["print_kvvalue"] = "true";
        try {
            readerInitParam.schema = indexlib::config::IndexPartitionSchema::Load(request.schemapath(), false);
        } catch (...) {
            AUTIL_LOG(ERROR, "load schema [%s] failed", schemaPath.c_str());
            return;
        }
        if (!readerInitParam.schema) {
            AUTIL_LOG(ERROR, "load schema [%s] failed", schemaPath.c_str());
            return;
        }
        AUTIL_LOG(INFO, "begin readSwift");
        // read
        readAndPrintDocs(request, readerInitParam, _impl->swiftClientCreator);
    } else {
        proto::ReadSwiftRequest request;
        proto::ReadSwiftResponse response;
        if (!fillSwiftRequestFromArgs(&request)) {
            return;
        }
        AUTIL_LOG(INFO, "read param:%s", request.DebugString().c_str());
        auto readerInitParam = createReaderInitParam(&request);
        readerInitParam.kvMap[build_service::config::READ_SRC_TYPE] = build_service::config::SWIFT_READ_SRC;
        AUTIL_LOG(INFO, "begin readSwift");
        readAndPrintDocs(request, readerInitParam, _impl->swiftClientCreator);
    }
}

void TraceServerWorker::test(const proto::TestRequest* request, proto::TestResponse* response)
{
    response->set_request(toJsonString(request));
    AUTIL_LOG(INFO, "readSwift [%s]", response->request().c_str());
    response->set_errorcode(proto::ErrorCode::ERROR_NONE);
    response->set_results(R"({docs: [{"key": "value"}, {"key":"v2"}]})");
    (*response->mutable_documentmap())["mapKey"] = "mapValue";
}

void TraceServerWorker::readSwift(const proto::ReadSwiftRequest* request, proto::ReadSwiftResponse* response)
{
    response->set_request(toJsonString(request));
    AUTIL_LOG(INFO, "readSwift [%s]", response->request().c_str());

    // prepare readerInitParam
    auto readerInitParam = createReaderInitParam(request);
    readerInitParam.kvMap[build_service::config::READ_SRC_TYPE] = build_service::config::SWIFT_READ_SRC;

    // read
    reader::RawDocumentReaderCreator readerCreator(_impl->swiftClientCreator);
    auto documentReader = createDocumentReader(readerInitParam, readerCreator, response);
    readDefaultRawDocument(documentReader, request, response);
}

void TraceServerWorker::readProcessedSwift(const proto::ReadProcessedSwiftRequest* request,
                                           proto::ReadSwiftResponse* response)
{
    response->set_request(toJsonString(request));
    AUTIL_LOG(INFO, "readProcessedSwift [%s]", response->request().c_str());

    // prepare readerInitParam
    auto readerInitParam = createReaderInitParam(request);
    readerInitParam.kvMap[build_service::config::READ_SRC_TYPE] = build_service::config::SWIFT_PROCESSED_READ_SRC;
    readerInitParam.kvMap["print_attribute"] = "true";
    readerInitParam.kvMap["print_summary"] = "true";
    readerInitParam.kvMap["print_kvvalue"] = request->safemode() ? "false" : "true";
    try {
        readerInitParam.schema = indexlib::config::IndexPartitionSchema::Load(request->schemapath(), false);
    } catch (...) {
    }
    if (!readerInitParam.schema) {
        response->set_errormsg("Load schema FAILED.");
        response->set_errorcode(proto::ErrorCode::ERROR_SWIFT_INIT);
        return;
    }

    // read
    reader::RawDocumentReaderCreator readerCreator(_impl->swiftClientCreator);
    auto documentReader = createDocumentReader(readerInitParam, readerCreator, response);
    if (readerInitParam.schema->GetTableType() == indexlib::tt_kv ||
        readerInitParam.schema->GetTableType() == indexlib::tt_kkv) {
        readKVDocument(documentReader, request, response);
    } else {
        readDefaultRawDocument(documentReader, request, response);
    }
}

} // namespace build_service::tools
