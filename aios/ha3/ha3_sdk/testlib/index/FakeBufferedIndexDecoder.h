#ifndef ISEARCH_FAKEBUFFEREDINDEXDECODER_H
#define ISEARCH_FAKEBUFFEREDINDEXDECODER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/format/buffered_index_decoder.h>

IE_NAMESPACE_BEGIN(index);

class FakeBufferedIndexDecoder : public BufferedIndexDecoder
{
public:
    FakeBufferedIndexDecoder(
            const index::PostingFormatOption& formatOption,
            autil::mem_pool::Pool *pool);

    ~FakeBufferedIndexDecoder();
private:
    FakeBufferedIndexDecoder(const FakeBufferedIndexDecoder &);
    FakeBufferedIndexDecoder& operator=(const FakeBufferedIndexDecoder &);
public:
    void Init(const std::string &docIdStr, const std::string &fieldStr);

public:
    virtual bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                                 docid_t &firstDocId, docid_t &lastDocId,
                                 ttf_t &currentTTF) override;
    
    virtual bool DecodeCurrentTFBuffer(tf_t *tfBuffer)  override;
    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) override;
    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer) override;
    uint32_t GetSeekedDocCount() const override;
private:
    std::vector<docid_t> _docIds;
    std::vector<fieldmap_t> _fieldMaps;
    uint32_t _blockCursor;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeBufferedIndexDecoder);

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEBUFFEREDINDEXDECODER_H
