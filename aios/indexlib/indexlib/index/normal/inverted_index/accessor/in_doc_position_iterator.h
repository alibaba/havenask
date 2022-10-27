#ifndef __INDEXLIB_IN_DOC_POSITION_ITERATOR_H
#define __INDEXLIB_IN_DOC_POSITION_ITERATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/common/error_code.h"
#include <tr1/memory>
#include <assert.h>

IE_NAMESPACE_BEGIN(index);

/**
 * @class InDocPositionIterator
 * @brief position iterator for in-doc position
 */
class InDocPositionIterator
{
public:
    virtual ~InDocPositionIterator() {}

public:
    /**
     * Seek position one bye one in current doc
     * @return position equal to or greater than /pos/
     */
    pos_t SeekPosition(pos_t pos) {
        pos_t result = 0;
        auto ec = SeekPositionWithErrorCode(pos, result);
        common::ThrowIfError(ec);
        return result;        
    }
    virtual common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) = 0;
    /**
     * Get position payload of current sought position 
     */
    virtual pospayload_t GetPosPayload() = 0;

    /**
     * Get the Section of current position
     */
    virtual sectionid_t GetSectionId() = 0;

    /**
     * Get the Section length of current position
     */
    virtual section_len_t GetSectionLength() = 0;

    /**
     * Get the section weight of current position
     */
    virtual section_weight_t GetSectionWeight() = 0;

    /**
     * Get the field id  of current position
     */
    virtual fieldid_t GetFieldId() = 0;

    /**
     * Get the field position in index of current position
     */
    virtual int32_t GetFieldPosition() = 0;

    /**
     * Return true if position-payload exists.
     */
    virtual bool HasPosPayload() const = 0;
    
};

typedef std::tr1::shared_ptr<InDocPositionIterator> InDocPositionIteratorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXENGINEIN_DOC_POSITION_ITERATOR_H
