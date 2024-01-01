/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * logfs.c
 */

#include <pthread.h>
#include "device.h"
#include "logfs.h"
#include <stdlib.h>

#define WCACHE_BLOCKS 32
#define RCACHE_BLOCKS 256
int running;

/**
 * Needs:
 *   pthread_create()
 *   pthread_join()
 *   pthread_mutex_init()
 *   pthread_mutex_destroy()
 *   pthread_mutex_lock()
 *   pthread_mutex_unlock()
 *   pthread_cond_init()
 *   pthread_cond_destroy()
 *   pthread_cond_wait()
 *   pthread_cond_signal()
 */

/* research the above Needed API and design accordingly */

struct CacheEntry {
    uint64_t deviceOffset;
    int validBit;
    char *data;
};

struct logfs {
    struct block_writing_place {
        uint64_t offset;
        char *place;
    } *block;

    struct write_cache {
        uint64_t to_device; 
        uint64_t to_write_cache; 
        char *data;
        char *data_;
        int available_blocks;
    } *write_cache;

    struct read_cache {
        struct CacheEntry entries[RCACHE_BLOCKS];
    } *read_cache;

    uint64_t device_block_size;
    uint64_t offset;
    struct device *device;

    pthread_t worker;
};

pthread_mutex_t AVAILABLE_BLOCKS;
pthread_cond_t FLUSH, PRODUCER;

void store_to_cache (uint64_t offset, void* data, struct logfs *logfs) {
    struct read_cache *read_cache = logfs->read_cache;
    int cache_offset = (offset / logfs->device_block_size) % RCACHE_BLOCKS;

    memcpy(read_cache->entries[cache_offset].data, data, logfs->device_block_size);
    read_cache->entries[cache_offset].validBit = 1;
    read_cache->entries[cache_offset].deviceOffset = offset;
}

void read_cache_free (struct read_cache* read_cache) {
    int i;
    for (i = 0; i < RCACHE_BLOCKS; i++) {
        FREE(read_cache->entries[i].data);
    }
    FREE(read_cache);
}
int read_cache_initializer (struct logfs *logfs) {
    struct read_cache* read_cache;
    int i;
    if(!(logfs -> read_cache = (struct read_cache*) malloc(sizeof(struct read_cache)))) {
        TRACE("Malloc failed");
        return 0;
    }
    read_cache = logfs->read_cache;
    for (i = 0; i < RCACHE_BLOCKS; i++) {
        read_cache->entries[i].deviceOffset = 0;
        read_cache->entries[i].validBit = 0;
        read_cache->entries[i].data = (char*) malloc(logfs->device_block_size);
        memset(read_cache->entries[i].data, 0, logfs->device_block_size);
    }
    return 1;
}
int write_cache_initializer (struct logfs *logfs) {
    logfs -> write_cache = (struct write_cache*) malloc(sizeof(struct write_cache));
    logfs -> write_cache -> to_device = 0;
    logfs -> write_cache -> to_write_cache = 0;
    logfs -> write_cache -> available_blocks = WCACHE_BLOCKS;
    if (!(logfs -> write_cache -> data_ = (char*)malloc((WCACHE_BLOCKS+1) * logfs->device_block_size))) {
        TRACE("Malloc failed");
        return 0;
    }
    memset(logfs ->write_cache -> data_, 0, (WCACHE_BLOCKS+1) * logfs->device_block_size);
    
    logfs->write_cache->data = (char*)memory_align(logfs->write_cache->data_, logfs->device_block_size);
    return 1;
}
int is_present_in_cache (uint64_t offset, struct logfs *logfs) {

    struct read_cache *read_cache = logfs->read_cache;

    int cache_offset = (offset / logfs->device_block_size) % RCACHE_BLOCKS;

    if ((read_cache->entries[cache_offset].deviceOffset == offset) 
            && (read_cache->entries[cache_offset].validBit))
        return cache_offset;
    else
        return -1; 
}

void* write_to_device(void *args) {
    struct logfs *logfs = (struct logfs*)args;
    while (running) {
        if(pthread_mutex_lock(&AVAILABLE_BLOCKS)) {
            TRACE("Could not acquire mutex");
            running = 0;
        }
        while(logfs->write_cache->available_blocks == WCACHE_BLOCKS && (running)) {
            pthread_cond_wait(&FLUSH, &AVAILABLE_BLOCKS);
            if (!running) {
                pthread_mutex_unlock(&AVAILABLE_BLOCKS);
                break;
            }
        }
        while (running) {
            if (logfs->write_cache->available_blocks == WCACHE_BLOCKS){
                pthread_mutex_unlock(&AVAILABLE_BLOCKS);
                break;
            }
            pthread_mutex_unlock(&AVAILABLE_BLOCKS);
            if(device_write(logfs->device, logfs->write_cache->data + logfs->write_cache->to_device, 
                            logfs->offset, 
                            logfs->device_block_size)) {

                TRACE("Device write failed");
                running = 0;
            }
            logfs->offset += logfs->device_block_size;
            logfs->write_cache->to_device = (logfs->write_cache->to_device + logfs->device_block_size) % 
                                            (WCACHE_BLOCKS * logfs->device_block_size);
            if (pthread_mutex_lock(&AVAILABLE_BLOCKS)) {
                TRACE("Could not acquire mutex");
                running = 0;
            }
            logfs->write_cache->available_blocks ++;
        }

        if (running) {
            if (pthread_mutex_unlock(&AVAILABLE_BLOCKS)) {
                TRACE("Could not release mutex");
                running = 0;
            }
            if (pthread_cond_signal(&PRODUCER)) {
                TRACE("Signalling failed");
                running = 0;
            }
        }
    }
    pthread_exit(NULL);
}

struct logfs *logfs_open(const char *pathname) {
    struct logfs *logfs_instance;
    struct device *new_device;

    if (!(logfs_instance = (struct logfs *)malloc(sizeof(struct logfs))))
        return NULL;

    if (!(new_device = device_open(pathname))) {
        logfs_close(logfs_instance);
        return NULL;
    }

    logfs_instance -> device_block_size = device_block(new_device);
    logfs_instance -> device = new_device;
    logfs_instance -> offset = 0;

    if (!(logfs_instance -> block = (struct block_writing_place*) 
                                    malloc(sizeof(struct block_writing_place)))) {
        logfs_close(logfs_instance);
        return NULL;
    }
    
    logfs_instance -> block -> offset = 0;

    if (!(logfs_instance -> block -> place = malloc(logfs_instance -> device_block_size))) {
        logfs_close(logfs_instance);
        return NULL;
    }
    
    memset(logfs_instance->block->place, 0, logfs_instance -> device_block_size);

    if( !write_cache_initializer(logfs_instance)) {
        logfs_close(logfs_instance);
        return NULL;
    }

    if( !read_cache_initializer(logfs_instance)) {
        logfs_close(logfs_instance);
        return NULL;
    }

    if(pthread_mutex_init(&AVAILABLE_BLOCKS, NULL)) {
        TRACE("Initializing mutex failed");
        return NULL;
    }
    if (pthread_cond_init(&FLUSH, NULL)) {
        TRACE("Initializing flush condition variable failed");
        return NULL;
    }
    if (pthread_cond_init(&PRODUCER, NULL)) {
        TRACE ("Initializing producer condition variable failed");
        return NULL;
    }

    running = 1;
    if(pthread_create(&logfs_instance->worker, NULL, write_to_device, (void*)logfs_instance)) {
        TRACE("Thread creation failed");
        return NULL;
    }
    
    return logfs_instance;
}


void logfs_close(struct logfs *logfs) {
    running = 0;
    if (logfs->worker) {
        pthread_cond_signal(&FLUSH);
        pthread_join(logfs->worker, NULL);
        pthread_mutex_destroy(&AVAILABLE_BLOCKS);
        pthread_cond_destroy(&FLUSH);
        pthread_cond_destroy(&PRODUCER);
    }

    if(logfs) {
        if (logfs->device)
            device_close(logfs->device);
        
        if (logfs->block->place)
            FREE(logfs->block->place);
        
        if (logfs->block)
            FREE(logfs->block);
        
        if (logfs->write_cache->data_)
            FREE(logfs->write_cache->data_);
        
        if (logfs->write_cache)
            FREE(logfs->write_cache);
        if (logfs->read_cache)
            read_cache_free(logfs->read_cache);

        FREE(logfs);
    }
}

int logfs_read(struct logfs *logfs, void *buf, uint64_t off, size_t len) {
    uint64_t starting_block;
    char *temp_buf, *orig;
    int no_of_blocks = 0;
    int filled_till_now;

    starting_block = off - (off % logfs->device_block_size);
    no_of_blocks = ((off - starting_block + len) / logfs->device_block_size) + 1;

    if (!(orig = malloc((no_of_blocks+1) * logfs->device_block_size))) {
        TRACE("Malloc failed");
        return -1;
    }
    temp_buf = memory_align(orig, logfs->device_block_size);
    memset(temp_buf, 0, no_of_blocks*logfs->device_block_size);

    if (logfs->block->offset > 0) {
        memcpy(logfs->write_cache->data + logfs->write_cache->to_write_cache, 
                logfs->block->place, 
                logfs->device_block_size);
        logfs->write_cache->to_write_cache = (logfs->write_cache->to_write_cache 
                                            + logfs->device_block_size) % 
                                            (WCACHE_BLOCKS * logfs->device_block_size);
        if (pthread_mutex_lock(&AVAILABLE_BLOCKS)) {
            TRACE("Acquiring mutex failed");
            return -1;
        }
        logfs->write_cache->available_blocks--;
        if (pthread_mutex_unlock(&AVAILABLE_BLOCKS)) {
            TRACE("Releasing mutex failed");
            return -1;
        }
        if (pthread_cond_signal(&FLUSH)) {
            TRACE("Signalling failed");
            return -1;
        }
        
    } else {
        if (pthread_cond_signal(&FLUSH)) {
            TRACE("Signalling failed");
            return -1;
        }
    }
    if (pthread_mutex_lock(&AVAILABLE_BLOCKS)) {
        TRACE("Acquiring mutex failed");
        return -1;
    }
    while (logfs->write_cache->available_blocks != WCACHE_BLOCKS) {
        pthread_cond_wait(&PRODUCER, &AVAILABLE_BLOCKS);
    }
    if (pthread_mutex_unlock(&AVAILABLE_BLOCKS)) {
        TRACE("Releasing mutex failed");
        return -1;
    }
    
    if (logfs->block->offset > 0)
        logfs->offset -= logfs->device_block_size;

    filled_till_now = 0;
    while (no_of_blocks) {
        int offset = is_present_in_cache(starting_block, logfs);
        if (offset == -1) {
            if (device_read(logfs->device, 
                        temp_buf + (filled_till_now * logfs->device_block_size), 
                        starting_block, 
                        logfs->device_block_size)) {
                TRACE("Device read failed");
                return -1;
            }
            store_to_cache(starting_block, 
                    temp_buf + (filled_till_now * logfs->device_block_size), 
                    logfs);
        }
        else {
            memcpy(temp_buf + (filled_till_now * logfs->device_block_size), 
                    logfs->read_cache->entries[offset].data, 
                    logfs->device_block_size);
        }
        starting_block += logfs->device_block_size;
        filled_till_now ++;
        no_of_blocks --;
    }
    memcpy((char*)buf, temp_buf + (off % logfs->device_block_size), len);
    
    FREE(orig);
    return 0;
}

int logfs_append(struct logfs *logfs, const void *buf, uint64_t len) {

    if (logfs->block->offset > 0){
        int offset = is_present_in_cache(logfs->offset, logfs);
        if (offset != -1) {
            logfs->read_cache->entries[offset].validBit = 0;
        }
    }
    if (len < (logfs->device_block_size - logfs->block->offset)) {
        memcpy(logfs->block->place + logfs->block->offset, (char*)buf, len);
        logfs->block->offset += len;
    }
    else {
        int no_of_blocks;
        int blocks_filled;

        uint64_t temp_len = logfs->device_block_size - logfs->block->offset;
        memcpy(logfs->block->place + logfs->block->offset, (char*)buf, temp_len);

        if (pthread_mutex_lock(&AVAILABLE_BLOCKS)) {
            TRACE("Acquiring mutex failed");
            return -1;
        }
        while (logfs->write_cache->available_blocks == 0) {
            pthread_cond_wait(&PRODUCER, &AVAILABLE_BLOCKS);
        }
        memcpy(logfs->write_cache->data + logfs->write_cache->to_write_cache, 
                    logfs->block->place, 
                    logfs->device_block_size);
        logfs->write_cache->to_write_cache = (logfs->write_cache->to_write_cache + logfs->device_block_size) 
                                            % (WCACHE_BLOCKS * logfs->device_block_size);
        logfs->block->offset = 0;

        len = len - temp_len;

        logfs->write_cache->available_blocks--;
        if (pthread_mutex_unlock(&AVAILABLE_BLOCKS)) {
            TRACE("Releasing mutex failed");
            return -1;
        }
        if (pthread_cond_signal(&FLUSH)) {
            TRACE("Signalling failed");
            return -1;
        }

        no_of_blocks = (len / logfs->device_block_size);
        blocks_filled = 0;
        while (no_of_blocks) {
            if (pthread_mutex_lock(&AVAILABLE_BLOCKS)) {
                TRACE("Acquiring mutex failed");
                return -1;
            }
            while (logfs->write_cache->available_blocks == 0) {
                pthread_cond_wait(&PRODUCER, &AVAILABLE_BLOCKS);
            }
            
            memcpy(logfs->write_cache->data + logfs->write_cache->to_write_cache, 
                        (char*)buf + temp_len + (blocks_filled*logfs->device_block_size), 
                        logfs->device_block_size);

            logfs->write_cache->to_write_cache = (logfs->write_cache->to_write_cache + 
                                                    logfs->device_block_size) % 
                                                    (WCACHE_BLOCKS * logfs->device_block_size);
            blocks_filled ++;
            no_of_blocks --;
            len = len - logfs->device_block_size;

            logfs->write_cache->available_blocks--;
            if (pthread_mutex_unlock(&AVAILABLE_BLOCKS)) {
                TRACE("Releasing mutex failed");
                return -1;
            }
            if (pthread_cond_signal(&FLUSH)) {
                TRACE("Signalling failed");
                return -1;
            }
        }
        
        memcpy(logfs->block->place, 
                (char*)buf + temp_len + ((blocks_filled)*logfs->device_block_size), 
                len);
        logfs->block->offset += len;
    }
    return 0;
}
