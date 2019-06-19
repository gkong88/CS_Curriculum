/*
 * mm_alloc.h
 *
 * A clone of the interface documented in "man 3 malloc".
 */

#pragma once

#include <stdlib.h>

void *mm_malloc(size_t size);
void *mm_realloc(void *ptr, size_t size);
void mm_free(void *ptr);

typedef struct memory_block {
    size_t size;
    struct memory_block *prev;
    struct memory_block *next;
    int free;
    void *mem_pointer;
} *s_mem_block_ptr;