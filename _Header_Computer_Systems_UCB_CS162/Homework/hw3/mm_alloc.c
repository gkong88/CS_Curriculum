/*
 * mm_alloc.c
 *
 * Stub implementations of the mm_* routines.
 */

#include "mm_alloc.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>


s_mem_block_ptr head = NULL;
s_mem_block_ptr tail = NULL;

/*
 * Memory block init
 */
s_mem_block_ptr mem_block_init(s_mem_block_ptr ptr) {
    ptr->next = NULL;
}

/*
 * Zero fills the entire memory block located at ptr.
 */
void static zero_fill(s_mem_block_ptr block_ptr) {
    memset(block_ptr -> mem_pointer, 0, block_ptr -> size);
}

/*
 * Attempts to split the memory block located at s_mem_block_ptr
 * if it has enough space.
 *
 * Otherwise, does nothing.
 */
void split_block(s_mem_block_ptr block_ptr, size_t size) {
    if (block_ptr->size - size <= sizeof(s_mem_block_ptr)) {
        return; // not enough extra space to make it worth while
    }
    else {
        s_mem_block_ptr new_block_ptr = block_ptr + size + sizeof(struct memory_block);
        new_block_ptr -> mem_pointer = new_block_ptr + sizeof(struct memory_block);
        new_block_ptr -> size = block_ptr -> size - size - sizeof(struct memory_block);
        new_block_ptr -> prev = block_ptr;
        new_block_ptr -> free = 1;
        new_block_ptr -> next = block_ptr -> next;
        if (new_block_ptr -> next != NULL) {
            new_block_ptr -> next -> prev = new_block_ptr;
        }
        block_ptr -> next = new_block_ptr;
        block_ptr -> size = size;
    }
}

/*
 * searches existing memory blocks for a free block of size_t size and returns it.
 *
 * If none exists, returns NULL
 */
void *first_fit(size_t size) {
    s_mem_block_ptr block_ptr = head;
    while (block_ptr != NULL) {
        if (block_ptr->size >= size) {
            split_block(block_ptr, size);
            return block_ptr;
        }
        else {
            block_ptr = block_ptr->next;
        }
    }
    return NULL;
}

/*
 * Expands heap memory to fit a new block of size_t.
 * Returns a pointer to start of new block
 *
 * Updates tail reference as necessary.
 */
void *get_new_block(size_t size) {
    void *mem_pointer = sbrk(size + sizeof(struct memory_block)); // alloc memory in the heap
    if (mem_pointer == -1) {
        return NULL; // failed, sbrk cannot extend heap further!
    }
    s_mem_block_ptr new_block_ptr = (s_mem_block_ptr) mem_pointer; // put your metadata struct at the start of it
    new_block_ptr -> free = 1; // fill in your meta data
    new_block_ptr -> mem_pointer = mem_pointer + sizeof(struct memory_block);
    new_block_ptr -> next = NULL;
    new_block_ptr -> prev = tail;
    new_block_ptr -> size = size;
    if (tail == NULL) {
        head = new_block_ptr;
    } else {
        tail -> next = new_block_ptr;
    }
    tail = new_block_ptr;
    return new_block_ptr;
}

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
    if (size == 0) {
        return NULL;
    }
    s_mem_block_ptr free_block_ptr;
    free_block_ptr = first_fit(head);
    if (free_block_ptr == NULL) { // first_fit failed
        free_block_ptr = get_new_block(size);
        if (free_block_ptr == NULL) {
            return NULL;
        }
    }
    zero_fill(free_block_ptr);
    free_block_ptr -> free = 0;
    return free_block_ptr -> mem_pointer;
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
    mm_free(ptr); // data is still there!
    if (size == 0) {
        return NULL;
    }
    else {
        s_mem_block_ptr old_block_ptr = (s_mem_block_ptr) ptr;
        s_mem_block_ptr new_block_ptr = mm_malloc(size);
        memcpy(new_block_ptr->mem_pointer, old_block_ptr->mem_pointer, size);
        return new_block_ptr;
    }
}
/*
 * Coalesces free blocks block_ptr and block_ptr prev into one block
 */
s_mem_block_ptr coalesce_back(s_mem_block_ptr block_ptr) {
    block_ptr -> next -> prev = block_ptr -> prev;  // update next bacl link to skip over current block
    block_ptr -> prev -> next = block_ptr -> next; // update prev next link to skip over current block
    block_ptr -> prev -> size = block_ptr -> prev -> size + block_ptr -> size + sizeof(s_mem_block_ptr); // size update
    return block_ptr -> prev;
}


/*
 * Frees memory block that starts at *ptr.
 * The recently freed block should coalesce whenever possible.
 *
 * If given a null pointer, does nothing
 */
void mm_free(void *ptr) {
    s_mem_block_ptr  block_ptr = (s_mem_block_ptr) ptr;
    if (ptr == NULL) return;
    block_ptr -> free = 1;
    if (block_ptr -> prev != NULL && block_ptr -> prev -> free == 1) {
        block_ptr = coalesce_back(block_ptr);
    }
    if (block_ptr -> next != NULL && block_ptr -> next -> free == 1) {
        coalesce_back(block_ptr -> next);
    }
}

