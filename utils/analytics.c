#include "analytics.h"

my_key_t *alloc_one_key(size_t key_size) {
    my_key_t *new = (my_key_t *)malloc(sizeof(my_key_t));
    assert(new);
    
    new->key = (char *)malloc(sizeof(char) * key_size);
    assert(new->key);

    new->next = NULL;
    return new;
}

key_list *alloc_key_list() {
    key_list *new = (key_list *)malloc(sizeof(key_list));
    assert(new);
    new->head_key = NULL;
    new->tail_key = NULL;
    return new;
}

key_list **all_keys_setup() {

    key_list *happy_hour_keys = alloc_key_list();
    key_list *happy_day_keys = alloc_key_list();
    key_list *active_hour_keys = alloc_key_list();
    key_list *active_day_keys = alloc_key_list();

    key_list **key_lists = (key_list **)malloc(sizeof(key_list *) * 4);
    assert(key_lists);
    key_lists[0] = happy_hour_keys;
    key_lists[1] = happy_day_keys;
    key_lists[2] = active_hour_keys;
    key_lists[3] = active_day_keys;

    return key_lists;
}

HashTable **all_ht_setup() {

    HashTable *happy_hour;
    HashTable *happy_day;
    HashTable *active_hour;
    HashTable *active_day;

    ht_setup(happy_hour, HOUR_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(happy_day, DATE_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(active_hour, HOUR_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);
    ht_setup(active_day, DATE_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);

    HashTable **hash_tables = (HashTable **) malloc(sizeof(HashTable *) * 4);
    assert(hash_tables);
    hash_tables[0] = happy_hour;
    hash_tables[1] = happy_day;
    hash_tables[2] = active_hour;
    hash_tables[3] = active_day;

    return hash_tables;

}

void destroy_all_ht (HashTable *hts) {
    /**
     * TODO: implement to clean up hashtables
    */

}

void insert_key(key_list *key_list, char *key, size_t size) {
    if (key_list->head_key) {
        my_key_t *new = alloc_one_key(size);
        memcpy(new->key, key, size);
        key_list->tail_key->next = new;
        key_list->tail_key = new;
    } else {
        my_key_t *new = alloc_one_key(size);
        memcpy(new->key, key, size);
        key_list->head_key = new;
        key_list->tail_key = new;
    }
}

void free_key_list(key_list **key_list) {
    my_key_t *curr = (*key_list)->head_key;
    my_key_t *prev = NULL;

    while (curr) {
        free(curr->key);
        curr->key = NULL;
        
        prev = curr;
        curr = curr->next;

        free(prev);
        prev = NULL;
    }
    free(*key_list);
    *key_list = NULL;
    return;
}

void process_tweet_data(HashTable **ht, key_list **keys, data_struct *data) {

    // Below pointer locations for ht and keys, have to be brute-forced
    HashTable *happy_hour = ht[0];
    HashTable *happy_day = ht[1];
    HashTable *active_hour = ht[2];
    HashTable *active_day = ht[3];

    key_list *happy_hour_keys = keys[0];
    key_list *happy_day_keys = keys[1];
    key_list *active_hour_keys = keys[2];
    key_list *active_day_keys = keys[3];

    data_struct *curr = data;
    int temp_int = 1;

    while (curr) {
        char **time_strings = time_struct_to_str(curr->time);
        if (curr == data) {
            if (curr->sentiment > -9990) {
                update_ht_and_key(happy_hour, happy_hour_keys, time_strings[0], &(curr->sentiment), 1);
                update_ht_and_key(happy_day, happy_day_keys, time_strings[1], &(curr->sentiment), 1);
            }
            update_ht_and_key(active_hour, active_hour_keys, time_strings[0], &temp_int, 0);
            update_ht_and_key(active_day, active_day_keys, time_strings[1], &temp_int, 0);
        } else {
            if (curr->sentiment > -9990) {
                update_ht_and_key(happy_hour, happy_hour_keys, time_strings[0], &(curr->sentiment), 1);
                update_ht_and_key(happy_day, happy_day_keys, time_strings[1], &(curr->sentiment), 1);
            }
            update_ht_and_key(active_hour, active_hour_keys, time_strings[0], NULL, 0);
            update_ht_and_key(active_day, active_day_keys, time_strings[1], NULL, 0);
        }

        free(time_strings[0]);
        free(time_strings[1]);
        time_strings[0] = NULL;
        time_strings[1] = NULL;

        free(time_strings);
        time_strings = NULL;

        curr = curr->next;
    }
}

MPI_Items *all_ht_to_lists(HashTable **hash_tables, key_list **key_lists) {

    HashTable *happy_hour = hash_tables[0];
    HashTable *happy_day = hash_tables[1];
    HashTable *active_hour = hash_tables[2];
    HashTable *active_day = hash_tables[3];

    key_list *happy_hour_keys = key_lists[0];
    key_list *happy_day_keys = key_lists[1];
    key_list *active_hour_keys = key_lists[2];
    key_list *active_day_keys = key_lists[3];

    MPI_Items *ret = (MPI_Items *)malloc (sizeof(MPI_Items));
    assert(ret);
    ret->happy_hour = NULL;
    ret->happy_day = NULL;
    ret->active_hour = NULL;
    ret->active_day = NULL;

    ret->happy_hour = (MPI_Sentiment_node *) ht_to_list(happy_hour, happy_hour_keys, 1);
    ret->happy_day = (MPI_Sentiment_node *) ht_to_list(happy_day, happy_day_keys, 1);
    ret->active_hour = (MPI_Count_node *) ht_to_list(active_hour, active_hour_keys, 1);
    ret->active_day = (MPI_Count_node *) ht_to_list(active_day, active_day_keys, 1);

    return ret;
}

void *ht_to_list(HashTable *ht, key_list *key_list, int item_type) { // 1 for sentiment, 0 for count

    if (item_type) {
        MPI_Sentiment_node *ret_node = (MPI_Sentiment_node *)malloc(sizeof(MPI_Sentiment_node) * ht->size); // Malloc provides contiguous blocks
        my_key_t *curr = key_list->head_key;
        int i = 0;
        while (curr) {
            ret_node[i].key = (char *)malloc(sizeof(char) * ht->key_size);
            memcpy(ret_node[i].key, curr->key, ht->key_size);
            ret_node[i].sentiment = *(long double *)ht_lookup(ht, curr->key);
            curr = curr->next;
        }
        return (void *)ret_node;
    } else {
        MPI_Count_node *ret_node = (MPI_Count_node *)malloc(sizeof(MPI_Count_node) * ht->size); // Malloc provides contiguous blocks
        my_key_t *curr = key_list->head_key;
        int i = 0;
        while (curr) {
            ret_node[i].key = (char *)malloc(sizeof(char) * ht->key_size);
            memcpy(ret_node[i].key, curr->key, ht->key_size);
            ret_node[i].count = *(int *)ht_lookup(ht, curr->key);
            curr = curr->next;
        }
        return (void *)ret_node;
    }
    
}

void consolidate_child_data(HashTable *ht, key_list *keys, int score_type, void *node, int list_size) {
    // time_type = 0 -> hour, time_type = 1 -> day, score_type = 1 -> sentiment, score_type = 0 -> count

    if (score_type) {
        MPI_Sentiment_node *head = (MPI_Sentiment_node *) node;
        for (int i=0;i < list_size;i++) {
            update_ht_and_key(ht, keys, head->key, (void *)&head->sentiment, 1);
            head += 1;
        }

    } else {
        MPI_Count_node *head = (MPI_Count_node *) node;
        for (int i=0;i < list_size;i++) {
            update_ht_and_key(ht, keys, head->key, (void *)&head->count, 0);
        }
    }
    return;

}

ret_struct *process_tweets(data_struct *data) {
    
    HashTable happy_hour;
    HashTable happy_day;
    HashTable active_hour;
    HashTable active_day;

    ht_setup(&happy_hour, HOUR_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(&happy_day, DATE_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(&active_hour, HOUR_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);
    ht_setup(&active_day, DATE_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);

    key_list *happy_hour_keys = alloc_key_list();
    key_list *happy_day_keys = alloc_key_list();
    key_list *active_hour_keys = alloc_key_list();
    key_list *active_day_keys = alloc_key_list();

    int most_active_day_count = 1, most_active_hour_count = 1;

    char *most_happy_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
    char *most_happy_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
    char *most_active_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
    char *most_active_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);

    data_struct *curr = data;

    while (curr) {
        char **time_strings = time_struct_to_str(curr->time);
        if (curr == data) {
            if (curr->sentiment > -9990) {
                update_ht_and_key(&happy_hour, happy_hour_keys, time_strings[0], &(curr->sentiment), 1);
                update_ht_and_key(&happy_day, happy_day_keys, time_strings[1], &(curr->sentiment), 1);
            }
            update_ht_and_key(&active_hour, active_hour_keys, time_strings[0], &most_active_hour_count, 0);
            update_ht_and_key(&active_day, active_day_keys, time_strings[1], &most_active_day_count, 0);
        } else {
            if (curr->sentiment > -9990) {
                update_ht_and_key(&happy_hour, happy_hour_keys, time_strings[0], &(curr->sentiment), 1);
                update_ht_and_key(&happy_day, happy_day_keys, time_strings[1], &(curr->sentiment), 1);
            }
            update_ht_and_key(&active_hour, active_hour_keys, time_strings[0], NULL, 0);
            update_ht_and_key(&active_day, active_day_keys, time_strings[1], NULL, 0);
        }

        free(time_strings[0]);
        free(time_strings[1]);
        time_strings[0] = NULL;
        time_strings[1] = NULL;

        free(time_strings);
        time_strings = NULL;

        curr = curr->next;
    }

    find_most(&happy_hour, happy_hour_keys, most_happy_hour, 1);
    find_most(&happy_day, happy_day_keys, most_happy_day, 1);
    find_most(&active_hour, active_hour_keys, most_active_hour, 0);
    find_most(&active_day, active_day_keys, most_active_day, 0);

    ret_struct *ret = alloc_ret_struct();
    memcpy(ret->happy_hour_time, most_happy_hour, HOUR_STR_LEN);
    memcpy(ret->happy_day_time, most_happy_day, DATE_STR_LEN);
    memcpy(ret->active_hour_time, most_active_hour, HOUR_STR_LEN);
    memcpy(ret->active_day_time, most_active_day, DATE_STR_LEN);

    ret->happy_hour_sentiment = *(long double *)ht_lookup(&happy_hour, ret->happy_hour_time);
    ret->happy_day_sentiment = *(long double *)ht_lookup(&happy_day, ret->happy_day_time);
    ret->active_hour_count = *(int *)ht_lookup(&active_hour, ret->active_hour_time);
    ret->active_day_count = *(int *)ht_lookup(&active_day, ret->active_day_time);

    free(most_happy_hour);
    free(most_happy_day);
    free(most_active_hour);
    free(most_active_day);
    
    ht_destroy(&happy_hour);
    ht_destroy(&happy_day);
    ht_destroy(&active_hour);
    ht_destroy(&active_day);

    free_key_list(&happy_hour_keys);
    free_key_list(&happy_day_keys);
    free_key_list(&active_hour_keys);
    free_key_list(&active_day_keys);

    return ret;

}

int update_ht_and_key(HashTable *ht, key_list *key_list, char *key, void *value, int data_type) { // data_type = 1 -> sentiment, 0 -> count;
    if (data_type) {
        long double *sentiment = (long double *) ht_lookup(ht, (void *)key);
        if (sentiment) { // Key is found in Hashtable, no need to insert to list
            long double temp = *sentiment;
            temp += *(long double *)value;
            ht_insert(ht, (void *)key, &temp);
        } else {
            // Key is not found in Hashtable
            ht_insert(ht, (void *)key, value);
            insert_key(key_list, key, ht->key_size);
        }

        return HT_SUCCESS;

    } else {
        int *count = (int *) ht_lookup(ht, (void *)key);
        int temp = 1;
        if (count) { // Key is found in Hashtable, no need to insert to list
            temp = *count;
            temp += 1;
            ht_insert(ht, (void *)key, &temp);
        } else { 
            // Key is not found in Hashtable
            ht_insert(ht, (void *)key, &temp);
            insert_key(key_list, key, ht->key_size);
        }

        return HT_SUCCESS;
    }

    return HT_ERROR;
}

void find_most(HashTable *ht, key_list *key_list, char *leading_time, int data_type) { // data_type = 1 -> sentiment, 0 -> count;
    char *max_time = key_list->head_key->key;
    void *max_val = ht_lookup(ht, key_list->head_key->key);

    my_key_t *curr = key_list->head_key->next;
    while (curr) {
        void *val = ht_lookup(ht, curr->key);
        if (data_type) {
            if (*(long double *)val > *(long double *)max_val) {
                max_val = val;
                max_time = curr->key;
            }
        } else {
            if (*(int *)val > *(int *)max_val) {
                max_val = val;
                max_time = curr->key;
            }
        }

        curr = curr->next;
    }

    memcpy(leading_time, max_time, ht->key_size);
    return;
}
