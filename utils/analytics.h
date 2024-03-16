#ifndef ANALYTICS_H
#define ANALYTICS_H

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include "time_utils.h"
#include "data.h"
#include "ht.h"

typedef struct key {
    char *key;
    struct key *next;
} my_key_t;

typedef struct key_list {
    my_key_t *head_key;
    my_key_t *tail_key;
} key_list;

my_key_t *alloc_one_key(size_t key_size);
key_list *alloc_key_list();
void insert_key(key_list *key_list, char *key, size_t size);
void free_key_list(key_list **key_list);
ret_struct *process_tweets(data_struct *data);
int update_ht_and_key(HashTable *ht, key_list *key_list, char *key, void *value, int data_type);
void find_most(HashTable *ht, key_list *key_list, char *leading_time, int data_type);

#endif