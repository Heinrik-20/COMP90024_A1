#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include "utils/time_utils.h"
#include "utils/data.h"
#include "utils/analytics.h"

#define MAX_READS 10000

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);

    HashTable **hash_tables = all_ht_setup(); // This ensures that the proper order of the Hashtables
    key_list **all_keys = all_keys_setup();

    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    FILE *fp = NULL;
    size_t rowlen = 0;
    int skipped_first = 0;

    if (rank == 0) {
        fp = fopen(argv[1], "r");
    }

    data_struct *data = NULL;
    int end = 0;
    int ended_size = -1;
    while (end != -1) {
        int lineRecvLen = 0;
        char *lineReceived;
        if (rank == 0) {
            // Skip first row
            if (!skipped_first) {
                char *starting_line = NULL;
                getline_clean(&starting_line, &rowlen, fp);
                free(starting_line);
                starting_line = NULL;
                skipped_first = 1;
            }

            char **linesToSend = (char **) calloc(size, sizeof(char *));
            int *rowlens = malloc(size * sizeof(int));
            int *displacements = calloc(size, sizeof(int));

            assert(linesToSend);
            assert(rowlens);
            assert(displacements);

            for (int i = 0; i < size; i++) {
                linesToSend[i] = NULL;
                rowlens[i] = 0;
                end = getline_clean(&linesToSend[i], &rowlen, fp);
                rowlens[i] = (int) rowlen;
                if (end == -1) {
                    ended_size = i;
                    break;
                } else {
                    ended_size = i + 1;
                }
            }

            // figure out where each row begins and ends (prefix sum)
            displacements[0] = 0;
            for (int i = 0; i < size && i < ended_size; i++) {
                displacements[i] = displacements[i - 1] + rowlens[i - 1];
            }

            char *strings_to_send = (char *) malloc(sizeof(char) * (displacements[size - 1] + rowlens[size - 1]));
            assert(strings_to_send);
            for (int i = 0; i < size && i < ended_size; i++) {
                memcpy(strings_to_send + displacements[i], linesToSend[i], rowlens[i]);
            }

            if (ended_size < size) {
                for (int i = ended_size; i < size; i++) {
                    displacements[i] = 0;
                    rowlens[i] = 0;

                    printf("Ended size less than total size\n");
                }
            }

            MPI_Scatter(rowlens, 1, MPI_INT,
                        &lineRecvLen, 1, MPI_INT,
                        0, MPI_COMM_WORLD);

            if (lineRecvLen == -1) {
                break;
            }

            lineReceived = malloc(lineRecvLen * sizeof(char));
            assert(lineReceived);

            /**
             * TODO: need to refactor this to account for zero lengths, error occurs when there is nothing to send across to the other nodes.
             * 
            */
            MPI_Scatterv(strings_to_send, rowlens, displacements, MPI_CHAR,
                         lineReceived, lineRecvLen, MPI_CHAR,
                         0, MPI_COMM_WORLD);

            for (int i = 0; i < size && i < ended_size; i++) {
                free(linesToSend[i]);
                linesToSend[i] = NULL;
            }

            free(linesToSend);
            linesToSend = NULL;

            free(strings_to_send);
            strings_to_send = NULL;

            free(rowlens);
            rowlens = NULL;

            free(displacements);
            displacements = NULL;


        } else {
            MPI_Scatter(NULL, 1, MPI_INT, &lineRecvLen, 1, MPI_INT, 0, MPI_COMM_WORLD);

            if (lineRecvLen == 0) {
                break;
            }

            lineReceived = malloc(lineRecvLen * sizeof(char));
            assert(lineReceived);

            MPI_Scatterv(NULL, NULL, NULL, MPI_CHAR, lineReceived, lineRecvLen, MPI_CHAR, 0, MPI_COMM_WORLD);
        }

        /**
         *  TODO: Deal with line received information
         *      1. convert to data_struct
         *      2. Store into hashtable
         */
        data = read_data_char(lineReceived, lineRecvLen);
        process_tweet_data(hash_tables, all_keys, data);

        free(lineReceived);
        lineReceived = NULL;
        free_data_struct_list(&data);
        data = NULL;

    }

    // After the reading of the file has ended and all information has been processed
    /**
     *  TODO: Send Hashtable back to ROOT 
     *      1. Commit Type Nodes and create a contiguous type
     *      2. Send it back 
     */
    MPI_Datatype sentiment_node, count_node;
    MPI_Datatype sentiment_hour_arr, sentiment_day_arr, count_hour_arr, count_day_arr;
    MPI_Datatype list_size_arr;
    MPI_Datatype sentiment_types[2] = {MPI_LONG_DOUBLE, MPI_CHAR}, count_types[2] = {MPI_INT, MPI_CHAR};
    int lengths[] = {1, HOUR_STR_LEN}; // Here we just use the longest string we can find
    MPI_Aint displacements[2];

    MPI_Sentiment_node dummy_sentiment;
    MPI_Count_node dummy_count;
    MPI_Aint base_address;

    MPI_Get_address(&dummy_sentiment, &base_address);
    MPI_Get_address(&dummy_sentiment.key[0], &displacements[1]);
    MPI_Get_address(&dummy_sentiment.sentiment, &displacements[0]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Type_create_struct(2, lengths, displacements, sentiment_types, &sentiment_node);
    MPI_Type_commit(&sentiment_node);

    MPI_Get_address(&dummy_count, &base_address);
    MPI_Get_address(&dummy_count.key[0], &displacements[1]);
    MPI_Get_address(&dummy_count.count, &displacements[0]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Type_create_struct(2, lengths, displacements, count_types, &count_node);
    MPI_Type_commit(&count_node);

    MPI_Type_contiguous(hash_tables[0]->size, sentiment_node, &sentiment_hour_arr);
    MPI_Type_commit(&sentiment_hour_arr);
    MPI_Type_contiguous(hash_tables[1]->size, sentiment_node, &sentiment_day_arr);
    MPI_Type_commit(&sentiment_day_arr);
    MPI_Type_contiguous(hash_tables[2]->size, count_node, &count_hour_arr);
    MPI_Type_commit(&count_hour_arr);
    MPI_Type_contiguous(hash_tables[3]->size, count_node, &count_day_arr);
    MPI_Type_commit(&count_day_arr);

    // 0 -> happy hour, 1 -> happy day, 2 -> active hour, 3 -> active day
    int hash_list_sizes[] = {hash_tables[0]->size, hash_tables[1]->size, hash_tables[2]->size, hash_tables[3]->size};
    MPI_Type_contiguous(4, MPI_INT, &list_size_arr);
    MPI_Type_commit(&list_size_arr);

    MPI_Items *items_to_send = all_ht_to_lists(hash_tables, all_keys);

    if (rank == 0) {
        /**
         * TODO: Handle strings and store information
         *      1. Implement string_to_ht()
         *      2. Store additional new keys, and new scores (sentiment score or active counts)
         *      3. Aggregate and find_most();
         *      4. Clean up hashtable and linked list of keys
        */
        MPI_Sentiment_node *temp_happy_hour;
        MPI_Sentiment_node *temp_happy_day;
        MPI_Count_node *temp_active_hour;
        MPI_Count_node *temp_active_day;

        // Allocate memory for receiving the sizes of the lists of other processors
        int *list_sizes = malloc(size * 4 * sizeof(int));

        MPI_Gather(hash_list_sizes, 1, list_size_arr,
                   list_sizes, 1, list_size_arr,
                   0, MPI_COMM_WORLD);

        // 0 -> happy hour, 1 -> happy day, 2 -> active hour, 3 -> active day
        int temp_sizes[] = {0, 0, 0, 0};
        // Aggregate the sizes to be used later in memory allocation for the arrays that will receive the data
        for (int i = 0; i < size; i++) {
            temp_sizes[0] += list_sizes[4 * i + 0];
            temp_sizes[1] += list_sizes[4 * i + 1];
            temp_sizes[2] += list_sizes[4 * i + 2];
            temp_sizes[3] += list_sizes[4 * i + 3];
//            for (int j = 0; j < 4; j++) {
//                printf("%d ", list_sizes[4 * i + j]);
//            }
//            printf("\n");
        }


        temp_happy_hour = (MPI_Sentiment_node *) malloc(sizeof(MPI_Sentiment_node) * temp_sizes[0]);
        temp_happy_day = (MPI_Sentiment_node *) malloc(sizeof(MPI_Sentiment_node) * temp_sizes[1]);
        temp_active_hour = (MPI_Count_node *) malloc(sizeof(MPI_Count_node) * temp_sizes[2]);
        temp_active_day = (MPI_Count_node *) malloc(sizeof(MPI_Count_node) * temp_sizes[3]);
        MPI_Gather(items_to_send->happy_hour, 1, sentiment_hour_arr,
                   temp_happy_hour, 1, sentiment_hour_arr,
                   0, MPI_COMM_WORLD);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < 4; j++) {
                printf("%Lf ", temp_happy_hour[4 * i + j].sentiment);
            }
            printf("\n");
        }

//        MPI_Gather(items_to_send->happy_day, 1, sentiment_day_arr,
//                   temp_happy_day, 1, sentiment_day_arr,
//                   0, MPI_COMM_WORLD);
//        MPI_Gather(items_to_send->active_hour, 1, count_hour_arr,
//                   temp_active_hour, 1, count_hour_arr,
//                   0, MPI_COMM_WORLD);
//        MPI_Gather(items_to_send->active_day, 1, count_day_arr,
//                   temp_active_day, 1, count_day_arr,
//                   0, MPI_COMM_WORLD);

        // Deal with each incoming MPI nodes, be sure to read in the string properly.
        /**
         * This needs to be in the form fn(root_hashtable, root_keys, data_type, void *node_type)
        */
        consolidate_child_data(hash_tables[0], all_keys[0], 1, (void *) temp_happy_hour, list_sizes[0]);
        consolidate_child_data(hash_tables[1], all_keys[1], 1, (void *) temp_happy_day, list_sizes[1]);
        consolidate_child_data(hash_tables[2], all_keys[2], 0, (void *) temp_active_hour, list_sizes[2]);
        consolidate_child_data(hash_tables[3], all_keys[3], 0, (void *) temp_active_day, list_sizes[3]);
        free(temp_happy_hour);
        free(temp_happy_day);
        free(temp_active_hour);
        free(temp_active_day);


        char *final_happy_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
        char *final_happy_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
        char *final_active_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
        char *final_active_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
        find_most(hash_tables[0], all_keys[0], final_happy_hour, 1);
        find_most(hash_tables[1], all_keys[1], final_happy_day, 1);
        find_most(hash_tables[2], all_keys[2], final_active_hour, 0);
        find_most(hash_tables[3], all_keys[3], final_active_day, 0);

        printf("Most Happy Hour: %s, sentiment: %.2LF\n", final_happy_hour,
               *(long double *) ht_lookup(hash_tables[0], final_happy_hour));
        printf("Most Happy Day: %s, sentiment: %.2LF\n", final_happy_day,
               *(long double *) ht_lookup(hash_tables[1], final_happy_day));
        printf("Most Active Hour: %s, count: %d\n", final_active_hour,
               *(int *) ht_lookup(hash_tables[2], final_active_hour));
        printf("Most Active Day: %s, count: %d\n", final_active_day,
               *(int *) ht_lookup(hash_tables[3], final_active_day));

        fclose(fp);
    } else {
        /**
         * TODO: Handle strings and store informations
         *      1. Send items back to root
         *      2. Clean up hashtable and linkedlist of keys
        */
        MPI_Gather(hash_list_sizes, 1, list_size_arr,
                   NULL, 0, list_size_arr,
                   0, MPI_COMM_WORLD);
        MPI_Gather(items_to_send->happy_hour, 1, sentiment_hour_arr,
                   NULL, 0, sentiment_hour_arr,
                   0, MPI_COMM_WORLD);
//        MPI_Gather(items_to_send->happy_day, 1, sentiment_day_arr,
//                   NULL, 0, sentiment_day_arr,
//                   0, MPI_COMM_WORLD);
//        MPI_Gather(items_to_send->active_hour, 1, count_hour_arr,
//                   NULL, 0, count_hour_arr,
//                   0, MPI_COMM_WORLD);
//        MPI_Gather(items_to_send->active_day, 1, count_day_arr,
//                   NULL, 0, count_day_arr,
//                   0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}