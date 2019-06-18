/*
 * mm_alloc.c
 *
 * Stub implementations of the mm_* routines.
 */

#include "mm_alloc.h"
#include <stdlib.h>

/*
 * Allocates memory of size_t size
 * and returns a pointer to the beginning of the allocated space (NOT the metadata header)
 * Allocated block will be zero-filled for privacy
 *
 * Returns NULL on failure.
 * Returns NULL if size_t size == 0
 */
void *mm_malloc(size_t size) {
    /*
     * Functions attempts first fit among free memory blocks
     * already existent in memory heap.
     * If a free block of size >= size exists, then the block
     * will be split of possible s.t. the allocation is exactly
     * the requested size and a smaller free block is partitioned out.
     *
     * If first-fit fails, creates more space as necessary.
     */
    /* YOUR CODE HERE */
    return NULL;
}

/*
 * Reassigns memory block to a memory block of size_t size.
 *
 * Returns NULL on failure to assign a memory block.
 * If size == 0, free's memory block and returns NULL.
 * If ptr == NULL, assigns memory block as necessary.
 * If ptr == NULL and size == 0, returns NULL
 */
void *mm_realloc(void *ptr, size_t size) {
    /*
     * Phase 1: Frees memory at ptr.
     * Phase 2: Allocates memory of size_t
     * Phase 3: Copies data over
     */
    /* YOUR CODE HERE */
    return NULL;
}

/*
 * Frees memory block that starts at *ptr.
 * The recently freed block should coalesce whenever possible.
 *
 * If given a null pointer, does nothing
 */
void mm_free(void *ptr) {
    /* YOUR CODE HERE */
}

/*
 * Zero fills the entire memory block located at ptr.
 */
void static zero_fill(void *ptr) {

}

/*
 * Free's memory block. Coalesces adjacent memory blocks if possible.
 */
void static free_and_coalesce(void *ptr) {

}
