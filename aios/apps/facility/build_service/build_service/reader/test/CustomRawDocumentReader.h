#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class CustomRawDocumentReader : public RawDocumentReader
{
public:
    CustomRawDocumentReader();
    CustomRawDocumentReader(const CustomRawDocumentReader& other);
    ~CustomRawDocumentReader();

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;

protected:
    RawDocumentReader::ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override;

private:
    struct RawDocDesc {
        std::string docId;
        int64_t offset;
    };

private:
    size_t _cursor;
    std::vector<RawDocDesc> _documents;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::reader
